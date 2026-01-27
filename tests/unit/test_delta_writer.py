"""Unit tests for DeltaTableWriter."""

from unittest.mock import MagicMock, call, patch

import polars as pl
import pytest

from sql_databricks_bridge.core.delta_writer import DeltaTableWriter, WriteResult


@pytest.fixture
def mock_client():
    """Create a mocked DatabricksClient."""
    client = MagicMock()
    client.upload_dataframe.return_value = "/Volumes/main/default/vol/_staging/cl_q1.parquet"
    client.execute_sql.return_value = []
    client.delete_file.return_value = None
    return client


@pytest.fixture
def writer(mock_client):
    """Create DeltaTableWriter with mocked client and settings."""
    with patch("sql_databricks_bridge.core.delta_writer.get_settings") as mock_settings:
        mock_db = MagicMock()
        mock_db.catalog = "main"
        mock_db.schema_name = "default"
        mock_db.volume_path = "/Volumes/main/default/vol"
        mock_settings.return_value.databricks = mock_db

        w = DeltaTableWriter(client=mock_client)
    return w


class TestResolveTableName:
    def test_resolve_table_name(self, writer):
        result = writer.resolve_table_name("customers", "br")
        assert result == "main.default.br_customers"

    def test_resolve_table_name_custom_catalog_schema(self, writer):
        result = writer.resolve_table_name(
            "customers", "cl", catalog="kpi_dev_01", schema="bronze"
        )
        assert result == "kpi_dev_01.bronze.cl_customers"


class TestWriteDataFrame:
    def test_write_dataframe_success(self, writer, mock_client):
        df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        result = writer.write_dataframe(df, "customers", "br")

        assert isinstance(result, WriteResult)
        assert result.table_name == "main.default.br_customers"
        assert result.rows == 3
        assert result.duration_seconds >= 0

        # Verify call order: upload → execute_sql → delete
        mock_client.upload_dataframe.assert_called_once()
        staging_path = mock_client.upload_dataframe.call_args[0][1]
        assert "_staging/br_customers.parquet" in staging_path

        mock_client.execute_sql.assert_called_once()
        sql = mock_client.execute_sql.call_args[0][0]
        assert "CREATE OR REPLACE TABLE main.default.br_customers" in sql
        assert "read_files(" in sql

        mock_client.delete_file.assert_called_once_with(staging_path)

    def test_write_dataframe_empty(self, writer, mock_client):
        df = pl.DataFrame({"id": pl.Series([], dtype=pl.Int64)})

        result = writer.write_dataframe(df, "empty_table", "cl")

        assert result.rows == 0
        assert result.table_name == "main.default.cl_empty_table"
        mock_client.upload_dataframe.assert_called_once()

        sql = mock_client.execute_sql.call_args[0][0]
        assert "LIMIT 0" in sql

    def test_write_dataframe_cleanup_failure(self, writer, mock_client):
        """Cleanup failure should log warning, not raise."""
        mock_client.delete_file.side_effect = Exception("delete failed")
        df = pl.DataFrame({"id": [1]})

        # Should not raise
        result = writer.write_dataframe(df, "q1", "cl")
        assert result.rows == 1

    def test_write_dataframe_custom_catalog_schema(self, writer, mock_client):
        df = pl.DataFrame({"x": [1]})

        result = writer.write_dataframe(
            df, "sales", "mx", catalog="kpi_prd_01", schema="bronze"
        )

        assert result.table_name == "kpi_prd_01.bronze.mx_sales"
        sql = mock_client.execute_sql.call_args[0][0]
        assert "kpi_prd_01.bronze.mx_sales" in sql


class TestTableExists:
    def test_table_exists_true(self, writer, mock_client):
        mock_client.execute_sql.return_value = [{"col_name": "id"}]
        assert writer.table_exists("main.default.br_customers") is True

    def test_table_exists_false(self, writer, mock_client):
        mock_client.execute_sql.side_effect = Exception("TABLE_OR_VIEW_NOT_FOUND")
        assert writer.table_exists("main.default.nonexistent") is False
