"""Unit tests for db.traceability_table module."""

from unittest.mock import MagicMock, call

import pytest

from sql_databricks_bridge.db.traceability_table import (
    ensure_traceability_tables,
    get_tag_tables,
    get_traceability_tag,
    insert_traceability_tag,
    list_traceability_tags,
)

TAGS_TABLE = "bridge.events.traceability_tags"
TABLES_TABLE = "bridge.events.traceability_tag_tables"


@pytest.fixture
def mock_client():
    client = MagicMock()
    client.execute_sql.return_value = []
    return client


class TestEnsureTraceabilityTables:
    def test_creates_both_tables(self, mock_client):
        ensure_traceability_tables(mock_client, TAGS_TABLE, TABLES_TABLE)
        assert mock_client.execute_sql.call_count == 2

    def test_tags_table_ddl(self, mock_client):
        ensure_traceability_tables(mock_client, TAGS_TABLE, TABLES_TABLE)
        first_sql = mock_client.execute_sql.call_args_list[0][0][0]
        assert "CREATE TABLE IF NOT EXISTS" in first_sql
        assert TAGS_TABLE in first_sql
        assert "tag_id" in first_sql
        assert "USING DELTA" in first_sql

    def test_tables_table_ddl(self, mock_client):
        ensure_traceability_tables(mock_client, TAGS_TABLE, TABLES_TABLE)
        second_sql = mock_client.execute_sql.call_args_list[1][0][0]
        assert "CREATE TABLE IF NOT EXISTS" in second_sql
        assert TABLES_TABLE in second_sql
        assert "full_name" in second_sql
        assert "USING DELTA" in second_sql


class TestInsertTraceabilityTag:
    def test_inserts_tag_and_tables(self, mock_client):
        tables = [
            {
                "table_name": "jatoscompra_new",
                "full_name": "catalog.schema.jatoscompra_new",
                "catalog": "hive_metastore",
                "schema_name": "chile_202501",
                "row_count": 150000,
                "size_bytes": None,
            }
        ]
        insert_traceability_tag(
            mock_client,
            TAGS_TABLE,
            TABLES_TABLE,
            tag_id="abc-123",
            run_id="run-001",
            country="chile",
            period=202501,
            stage="inicio-precios",
            triggered_by="user@test.com",
            started_at="2025-01-15T08:00:00",
            completed_at="2025-01-15T08:05:00",
            status="completed",
            tables=tables,
        )
        # One INSERT for the tag + one for each table
        assert mock_client.execute_sql.call_count == 2

    def test_tag_insert_sql_contains_values(self, mock_client):
        insert_traceability_tag(
            mock_client,
            TAGS_TABLE,
            TABLES_TABLE,
            tag_id="abc-123",
            run_id="run-001",
            country="chile",
            period=202501,
            stage="inicio-precios",
            triggered_by="user@test.com",
            started_at="2025-01-15T08:00:00",
            completed_at=None,
            status="completed",
            tables=[],
        )
        tag_sql = mock_client.execute_sql.call_args_list[0][0][0]
        assert "INSERT INTO" in tag_sql
        assert TAGS_TABLE in tag_sql
        assert "abc-123" in tag_sql
        assert "chile" in tag_sql
        assert "202501" in tag_sql
        assert "NULL" in tag_sql  # completed_at is NULL

    def test_no_table_inserts_when_empty_tables(self, mock_client):
        insert_traceability_tag(
            mock_client,
            TAGS_TABLE,
            TABLES_TABLE,
            tag_id="abc-123",
            run_id="run-001",
            country="chile",
            period=202501,
            stage="inicio-precios",
            triggered_by="user@test.com",
            started_at="2025-01-15T08:00:00",
            completed_at=None,
            status="completed",
            tables=[],
        )
        assert mock_client.execute_sql.call_count == 1  # only tag row


class TestListTraceabilityTags:
    def test_no_filter_no_where_clause(self, mock_client):
        mock_client.execute_sql.side_effect = [
            [{"cnt": 0}],
            [],
        ]
        rows, total = list_traceability_tags(mock_client, TAGS_TABLE)
        count_sql = mock_client.execute_sql.call_args_list[0][0][0]
        assert "WHERE" not in count_sql
        assert total == 0

    def test_country_filter_adds_where(self, mock_client):
        mock_client.execute_sql.side_effect = [
            [{"cnt": 2}],
            [],
        ]
        list_traceability_tags(mock_client, TAGS_TABLE, country="chile")
        count_sql = mock_client.execute_sql.call_args_list[0][0][0]
        assert "WHERE" in count_sql
        assert "country = 'chile'" in count_sql

    def test_period_filter(self, mock_client):
        mock_client.execute_sql.side_effect = [
            [{"cnt": 1}],
            [],
        ]
        list_traceability_tags(mock_client, TAGS_TABLE, period=202501)
        count_sql = mock_client.execute_sql.call_args_list[0][0][0]
        assert "period = 202501" in count_sql

    def test_returns_total(self, mock_client):
        mock_client.execute_sql.side_effect = [
            [{"cnt": 5}],
            [],
        ]
        _, total = list_traceability_tags(mock_client, TAGS_TABLE)
        assert total == 5


class TestGetTraceabilityTag:
    def test_returns_row_when_found(self, mock_client):
        mock_row = {"tag_id": "abc-123", "country": "chile"}
        mock_client.execute_sql.return_value = [mock_row]
        result = get_traceability_tag(mock_client, TAGS_TABLE, tag_id="abc-123")
        assert result == mock_row

    def test_returns_none_when_not_found(self, mock_client):
        mock_client.execute_sql.return_value = []
        result = get_traceability_tag(mock_client, TAGS_TABLE, tag_id="missing")
        assert result is None

    def test_sql_contains_tag_id(self, mock_client):
        mock_client.execute_sql.return_value = []
        get_traceability_tag(mock_client, TAGS_TABLE, tag_id="abc-123")
        sql = mock_client.execute_sql.call_args[0][0]
        assert "abc-123" in sql
        assert TAGS_TABLE in sql


class TestGetTagTables:
    def test_returns_table_rows(self, mock_client):
        mock_rows = [{"table_name": "t1", "tag_id": "abc-123"}]
        mock_client.execute_sql.return_value = mock_rows
        result = get_tag_tables(mock_client, TABLES_TABLE, tag_id="abc-123")
        assert result == mock_rows

    def test_sql_filters_by_tag_id(self, mock_client):
        mock_client.execute_sql.return_value = []
        get_tag_tables(mock_client, TABLES_TABLE, tag_id="abc-123")
        sql = mock_client.execute_sql.call_args[0][0]
        assert "abc-123" in sql
        assert TABLES_TABLE in sql
