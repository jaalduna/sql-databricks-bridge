"""Unit tests for sync validators."""

import polars as pl
import pytest

from sql_databricks_bridge.models.events import SyncEvent, SyncOperation
from sql_databricks_bridge.sync.validators import (
    ValidationError,
    build_where_clause,
    check_duplicate_primary_keys,
    validate_data_has_primary_keys,
    validate_delete_limit,
    validate_primary_keys,
    validate_source_table,
    validate_table_name,
)


class TestValidatePrimaryKeys:
    """Tests for validate_primary_keys."""

    def test_insert_no_pks_required(self):
        """INSERT doesn't require primary keys."""
        event = SyncEvent(
            event_id="1",
            operation=SyncOperation.INSERT,
            source_table="cat.sch.table",
            target_table="dbo.table",
            primary_keys=[],
        )
        # Should not raise
        validate_primary_keys(event)

    def test_update_requires_pks(self):
        """UPDATE requires primary keys."""
        event = SyncEvent(
            event_id="1",
            operation=SyncOperation.UPDATE,
            source_table="cat.sch.table",
            target_table="dbo.table",
            primary_keys=[],
        )

        with pytest.raises(ValidationError) as exc_info:
            validate_primary_keys(event)

        assert "Primary keys required" in str(exc_info.value)

    def test_delete_requires_pks(self):
        """DELETE requires primary keys."""
        event = SyncEvent(
            event_id="1",
            operation=SyncOperation.DELETE,
            source_table="cat.sch.table",
            target_table="dbo.table",
            primary_keys=[],
        )

        with pytest.raises(ValidationError):
            validate_primary_keys(event)

    def test_update_with_pks_valid(self):
        """UPDATE with primary keys is valid."""
        event = SyncEvent(
            event_id="1",
            operation=SyncOperation.UPDATE,
            source_table="cat.sch.table",
            target_table="dbo.table",
            primary_keys=["id"],
        )
        # Should not raise
        validate_primary_keys(event)


class TestValidateDataHasPrimaryKeys:
    """Tests for validate_data_has_primary_keys."""

    def test_all_pks_present(self):
        """All primary keys present in DataFrame."""
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        # Should not raise
        validate_data_has_primary_keys(df, ["id"], "event-1")

    def test_missing_pk_column(self):
        """Missing primary key column raises error."""
        df = pl.DataFrame({"name": ["a", "b"]})

        with pytest.raises(ValidationError) as exc_info:
            validate_data_has_primary_keys(df, ["id"], "event-1")

        assert "id" in str(exc_info.value)

    def test_multiple_pks(self):
        """Composite primary keys work."""
        df = pl.DataFrame({"id1": [1], "id2": [2], "value": ["x"]})

        # Should not raise
        validate_data_has_primary_keys(df, ["id1", "id2"], "event-1")


class TestCheckDuplicatePrimaryKeys:
    """Tests for check_duplicate_primary_keys."""

    def test_no_duplicates(self):
        """No duplicates returns 0."""
        df = pl.DataFrame({"id": [1, 2, 3]})

        result = check_duplicate_primary_keys(df, ["id"], "event-1")
        assert result == 0

    def test_with_duplicates(self):
        """Duplicates are counted correctly."""
        df = pl.DataFrame({"id": [1, 1, 2, 2, 2, 3]})

        result = check_duplicate_primary_keys(df, ["id"], "event-1")
        assert result == 3  # 1 extra for id=1, 2 extra for id=2

    def test_empty_df(self):
        """Empty DataFrame returns 0."""
        df = pl.DataFrame({"id": pl.Series([], dtype=pl.Int64)})

        result = check_duplicate_primary_keys(df, ["id"], "event-1")
        assert result == 0

    def test_empty_pks(self):
        """Empty primary keys returns 0."""
        df = pl.DataFrame({"id": [1, 1, 2]})

        result = check_duplicate_primary_keys(df, [], "event-1")
        assert result == 0


class TestValidateDeleteLimit:
    """Tests for validate_delete_limit."""

    def test_within_limit(self):
        """Rows within limit pass."""
        validate_delete_limit(100, 1000, "event-1")

    def test_at_limit(self):
        """Rows at limit pass."""
        validate_delete_limit(1000, 1000, "event-1")

    def test_over_limit(self):
        """Rows over limit raise error."""
        with pytest.raises(ValidationError) as exc_info:
            validate_delete_limit(1001, 1000, "event-1")

        assert "1001 > 1000" in str(exc_info.value)

    def test_no_limit(self):
        """No limit (None) allows any count."""
        validate_delete_limit(1000000, None, "event-1")


class TestValidateTableName:
    """Tests for validate_table_name."""

    def test_schema_and_table(self):
        """Schema.table format works."""
        schema, table = validate_table_name("dbo.MyTable")

        assert schema == "dbo"
        assert table == "MyTable"

    def test_table_only(self):
        """Table-only defaults to dbo schema."""
        schema, table = validate_table_name("MyTable")

        assert schema == "dbo"
        assert table == "MyTable"

    def test_too_many_parts(self):
        """Three-part name raises error."""
        with pytest.raises(ValidationError):
            validate_table_name("db.schema.table")


class TestValidateSourceTable:
    """Tests for validate_source_table."""

    def test_valid_format(self):
        """Three-part name works."""
        catalog, schema, table = validate_source_table("catalog.schema.table")

        assert catalog == "catalog"
        assert schema == "schema"
        assert table == "table"

    def test_invalid_format_two_parts(self):
        """Two-part name raises error."""
        with pytest.raises(ValidationError) as exc_info:
            validate_source_table("schema.table")

        assert "catalog.schema.table" in str(exc_info.value)

    def test_invalid_format_one_part(self):
        """One-part name raises error."""
        with pytest.raises(ValidationError):
            validate_source_table("table")


class TestBuildWhereClause:
    """Tests for build_where_clause."""

    def test_single_pk(self):
        """Single primary key works."""
        clause, params = build_where_clause(["id"], {"id": 123})

        assert "[id] = :pk_id" in clause
        assert params["pk_id"] == 123

    def test_multiple_pks(self):
        """Multiple primary keys work."""
        clause, params = build_where_clause(
            ["id1", "id2"],
            {"id1": 1, "id2": 2},
        )

        assert "[id1] = :pk_id1" in clause
        assert "[id2] = :pk_id2" in clause
        assert params["pk_id1"] == 1
        assert params["pk_id2"] == 2

    def test_pk_and_conditions(self):
        """Primary keys and extra conditions work."""
        clause, params = build_where_clause(
            ["id"],
            {"id": 1, "status": "active"},
        )

        assert "[id] = :pk_id" in clause
        assert "[status] = :cond_status" in clause
        assert params["pk_id"] == 1
        assert params["cond_status"] == "active"

    def test_empty_conditions(self):
        """Empty conditions returns 1=1."""
        clause, params = build_where_clause([], {})

        assert clause == "1=1"
        assert params == {}
