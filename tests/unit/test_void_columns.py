"""Tests for VOID column prevention via SQL Server schema enforcement."""

import datetime
import decimal

import polars as pl
import pytest

from sql_databricks_bridge.db.sql_server import (
    _PYODBC_TYPE_MAP,
    _rows_to_dataframe,
    _schema_from_cursor,
)
from sql_databricks_bridge.core.extractor import concat_chunks


# ---------------------------------------------------------------------------
# _schema_from_cursor
# ---------------------------------------------------------------------------


class TestSchemaFromCursor:
    """Tests for _schema_from_cursor type mapping."""

    def test_maps_all_known_types(self):
        """All entries in _PYODBC_TYPE_MAP should be correctly resolved."""
        description = [
            ("str_col", str, None, None, None, None, None),
            ("int_col", int, None, None, None, None, None),
            ("float_col", float, None, None, None, None, None),
            ("decimal_col", decimal.Decimal, None, None, None, None, None),
            ("datetime_col", datetime.datetime, None, None, None, None, None),
            ("date_col", datetime.date, None, None, None, None, None),
            ("bytes_col", bytes, None, None, None, None, None),
            ("bool_col", bool, None, None, None, None, None),
        ]
        schema = _schema_from_cursor(description)

        assert schema["str_col"] == pl.String
        assert schema["int_col"] == pl.Int64
        assert schema["float_col"] == pl.Float64
        assert schema["decimal_col"] == pl.Float64
        assert schema["datetime_col"] == pl.Datetime
        assert schema["date_col"] == pl.Date
        assert schema["bytes_col"] == pl.Binary
        assert schema["bool_col"] == pl.Boolean

    def test_unknown_type_defaults_to_string(self):
        """Unknown type codes should default to pl.String."""
        description = [
            ("mystery_col", memoryview, None, None, None, None, None),
        ]
        schema = _schema_from_cursor(description)
        assert schema["mystery_col"] == pl.String

    def test_empty_description(self):
        """Empty cursor description should return empty schema."""
        assert _schema_from_cursor([]) == {}


# ---------------------------------------------------------------------------
# _rows_to_dataframe
# ---------------------------------------------------------------------------


class TestRowsToDataframe:
    """Tests for _rows_to_dataframe with cursor_schema support."""

    def test_all_null_column_with_cursor_schema(self):
        """A column with all NULLs should use cursor_schema type, not pl.Null."""
        columns = ["id", "name", "value"]
        rows = [
            (1, "a", None),
            (2, "b", None),
            (3, "c", None),
        ]
        cursor_schema = {
            "id": pl.Int64,
            "name": pl.String,
            "value": pl.Float64,
        }
        df = _rows_to_dataframe(columns, rows, cursor_schema)

        assert df["value"].dtype == pl.Float64
        assert df["value"].null_count() == 3
        assert df["id"].dtype == pl.Int64
        assert df["name"].dtype == pl.String

    def test_all_null_column_without_cursor_schema(self):
        """Without cursor_schema, all-null column should remain pl.Null."""
        columns = ["id", "value"]
        rows = [(1, None), (2, None)]
        df = _rows_to_dataframe(columns, rows)

        assert df["value"].dtype == pl.Null

    def test_empty_rows_with_cursor_schema(self):
        """Empty result set should use cursor_schema types for schema."""
        columns = ["id", "amount", "created_at"]
        cursor_schema = {
            "id": pl.Int64,
            "amount": pl.Float64,
            "created_at": pl.Datetime,
        }
        df = _rows_to_dataframe(columns, [], cursor_schema)

        assert len(df) == 0
        assert df.schema["id"] == pl.Int64
        assert df.schema["amount"] == pl.Float64
        assert df.schema["created_at"] == pl.Datetime

    def test_empty_rows_without_cursor_schema(self):
        """Empty result set without cursor_schema should use Utf8."""
        columns = ["id", "name"]
        df = _rows_to_dataframe(columns, [])

        assert len(df) == 0
        assert df.schema["id"] == pl.Utf8
        assert df.schema["name"] == pl.Utf8

    def test_inferred_type_not_overridden(self):
        """When column has actual data, cursor_schema should NOT override
        the inferred type (Polars inference is more specific)."""
        columns = ["id", "value"]
        rows = [(1, 42.5), (2, 3.14)]
        cursor_schema = {
            "id": pl.Int64,
            "value": pl.Float64,
        }
        df = _rows_to_dataframe(columns, rows, cursor_schema)

        # Polars infers Float64 from actual float data — should stay Float64
        assert df["value"].dtype == pl.Float64
        assert df["value"].to_list() == [42.5, 3.14]

    def test_mixed_null_and_data_columns(self):
        """Only columns with all NULLs should be affected by cursor_schema."""
        columns = ["id", "null_col", "data_col"]
        rows = [
            (1, None, "hello"),
            (2, None, "world"),
        ]
        cursor_schema = {
            "id": pl.Int64,
            "null_col": pl.Date,
            "data_col": pl.String,
        }
        df = _rows_to_dataframe(columns, rows, cursor_schema)

        assert df["null_col"].dtype == pl.Date
        assert df["data_col"].dtype == pl.String
        assert df["data_col"].to_list() == ["hello", "world"]

    def test_cursor_schema_missing_column(self):
        """Columns not in cursor_schema should be left as-is."""
        columns = ["id", "extra"]
        rows = [(1, None)]
        cursor_schema = {"id": pl.Int64}  # "extra" not listed
        df = _rows_to_dataframe(columns, rows, cursor_schema)

        # "extra" not in cursor_schema → stays pl.Null
        assert df["extra"].dtype == pl.Null


# ---------------------------------------------------------------------------
# DeltaTableWriter._fix_null_columns
# ---------------------------------------------------------------------------


class TestFixNullColumns:
    """Tests for the DeltaTableWriter safety net."""

    def test_casts_null_columns_to_string(self):
        from sql_databricks_bridge.core.delta_writer import DeltaTableWriter

        df = pl.DataFrame({
            "name": ["a", "b"],
            "empty": [None, None],
        })
        assert df["empty"].dtype == pl.Null

        fixed = DeltaTableWriter._fix_null_columns(df)
        assert fixed["empty"].dtype == pl.String
        assert fixed["name"].dtype == pl.String  # already String, unchanged

    def test_no_null_columns_unchanged(self):
        from sql_databricks_bridge.core.delta_writer import DeltaTableWriter

        df = pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})
        fixed = DeltaTableWriter._fix_null_columns(df)
        assert fixed.schema == df.schema


# ---------------------------------------------------------------------------
# concat_chunks
# ---------------------------------------------------------------------------


class TestConcatChunksNullSafety:
    """Tests that concat_chunks casts residual Null columns to String."""

    def test_all_null_column_across_chunks(self):
        """If a column is Null in ALL chunks, it should become String."""
        chunk1 = pl.DataFrame({"id": [1], "empty": [None]})
        chunk2 = pl.DataFrame({"id": [2], "empty": [None]})
        assert chunk1["empty"].dtype == pl.Null

        result = concat_chunks([chunk1, chunk2])
        assert result["empty"].dtype == pl.String

    def test_null_resolved_by_other_chunk(self):
        """If one chunk has data, the Null column should be upgraded normally."""
        chunk1 = pl.DataFrame({"id": [1], "val": [None]})
        chunk2 = pl.DataFrame({"id": [2], "val": [3.14]})
        assert chunk1["val"].dtype == pl.Null

        result = concat_chunks([chunk1, chunk2])
        assert result["val"].dtype == pl.Float64

    def test_single_chunk_with_null(self):
        """Single chunk — should pass through (no concat, returns original)."""
        chunk = pl.DataFrame({"id": [1], "empty": [None]})
        result = concat_chunks([chunk])
        # Single chunk returns as-is (no concat happens)
        assert result["empty"].dtype == pl.Null


# ---------------------------------------------------------------------------
# _PYODBC_TYPE_MAP completeness
# ---------------------------------------------------------------------------


class TestPyodbcTypeMap:
    """Ensure the type map covers pyodbc's common type_code values."""

    def test_all_expected_types_present(self):
        expected = {str, int, float, decimal.Decimal, datetime.datetime, datetime.date, bytes, bool}
        assert set(_PYODBC_TYPE_MAP.keys()) == expected
