"""Integration tests for the sync flow (Databricks → SQL Server).

These tests verify the full sync flow:
1. Create events in Databricks event table
2. Process events via SyncOperator
3. Verify SQL Server operations were called
4. Update event status in Databricks

Run with: pytest tests/integration/test_sync_flow.py -v

Requires environment variables:
  - DATABRICKS_HOST
  - DATABRICKS_TOKEN (or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET)
  - DATABRICKS_CATALOG (e.g., "003-precios")
  - DATABRICKS_SCHEMA (e.g., "bronze")
  - DATABRICKS_WAREHOUSE_ID (for SQL execution)
"""

import os
import uuid
from datetime import datetime
from unittest.mock import MagicMock

import polars as pl
import pytest

from tests.conftest import requires_databricks


# Check for SQL warehouse availability
DATABRICKS_WAREHOUSE_AVAILABLE = bool(os.environ.get("DATABRICKS_WAREHOUSE_ID"))

requires_sql_warehouse = pytest.mark.skipif(
    not DATABRICKS_WAREHOUSE_AVAILABLE,
    reason="Requires DATABRICKS_WAREHOUSE_ID environment variable for SQL execution"
)

# Integration test catalog/schema from environment
CATALOG = os.environ.get("DATABRICKS_CATALOG", "003-precios")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "bronze-data")
# Use backticks for identifiers with special characters (hyphens)
EVENT_TABLE = f"`{CATALOG}`.`{SCHEMA}`.bridge_events_test"
SOURCE_TABLE = f"`{CATALOG}`.`{SCHEMA}`.bridge_source_test"


@requires_databricks
@requires_sql_warehouse
class TestSyncFlowIntegration:
    """End-to-end integration tests for sync flow."""

    @pytest.fixture
    def databricks_client(self):
        """Create real Databricks client."""
        from sql_databricks_bridge.db.databricks import DatabricksClient

        client = DatabricksClient()
        yield client

    @pytest.fixture
    def mock_sql_client(self):
        """Create mock SQL Server client to capture operations."""
        mock = MagicMock()
        mock.bulk_insert.return_value = 0
        mock.execute_write.return_value = 0
        mock.execute_query.return_value = pl.DataFrame({"cnt": [0]})
        return mock

    @pytest.fixture
    def test_run_id(self):
        """Generate unique ID for this test run."""
        return str(uuid.uuid4())[:8]

    @pytest.fixture
    def setup_test_tables(self, databricks_client, test_run_id):
        """Create test tables in Databricks and clean up after."""
        event_table = f"{EVENT_TABLE}_{test_run_id}"
        source_table = f"{SOURCE_TABLE}_{test_run_id}"

        # Create event table
        create_event_sql = f"""
        CREATE TABLE IF NOT EXISTS {event_table} (
            event_id STRING,
            operation STRING,
            source_table STRING,
            target_table STRING,
            primary_keys ARRAY<STRING>,
            status STRING,
            rows_expected INT,
            rows_affected INT,
            error_message STRING,
            created_at TIMESTAMP,
            processed_at TIMESTAMP
        )
        """

        # Create source data table
        create_source_sql = f"""
        CREATE TABLE IF NOT EXISTS {source_table} (
            id INT,
            name STRING,
            value DOUBLE
        )
        """

        databricks_client.execute_sql(create_event_sql)
        databricks_client.execute_sql(create_source_sql)

        yield {"event_table": event_table, "source_table": source_table}

        # Cleanup
        try:
            databricks_client.execute_sql(f"DROP TABLE IF EXISTS {event_table}")
            databricks_client.execute_sql(f"DROP TABLE IF EXISTS {source_table}")
        except Exception as e:
            print(f"Cleanup warning: {e}")

    def test_insert_event_flow(
        self, databricks_client, mock_sql_client, setup_test_tables
    ):
        """Test full INSERT event flow: create event → process → verify."""
        from sql_databricks_bridge.models.events import SyncEvent, SyncOperation
        from sql_databricks_bridge.sync.operations import SyncOperator

        event_table = setup_test_tables["event_table"]
        source_table = setup_test_tables["source_table"]

        # Step 1: Insert source data into Databricks
        insert_source = f"""
        INSERT INTO {source_table} VALUES
            (1, 'Alice', 100.0),
            (2, 'Bob', 200.0),
            (3, 'Charlie', 300.0)
        """
        databricks_client.execute_sql(insert_source)

        # Step 2: Create sync event in Databricks
        event_id = f"evt-{uuid.uuid4()}"
        insert_event = f"""
        INSERT INTO {event_table} VALUES (
            '{event_id}',
            'INSERT',
            '{source_table}',
            'dbo.target_table',
            ARRAY('id'),
            'pending',
            3,
            NULL,
            NULL,
            current_timestamp(),
            NULL
        )
        """
        databricks_client.execute_sql(insert_event)

        # Step 3: Read event from Databricks
        event_result = databricks_client.execute_sql(
            f"SELECT * FROM {event_table} WHERE event_id = '{event_id}'"
        )
        assert len(event_result) == 1
        assert event_result[0]["status"] == "pending"

        # Step 4: Create SyncEvent from Databricks data
        event = SyncEvent(
            event_id=event_id,
            operation=SyncOperation.INSERT,
            source_table=source_table,
            target_table="dbo.target_table",
            primary_keys=["id"],
            rows_expected=3,
        )

        # Step 5: Process event with mocked SQL Server
        mock_sql_client.bulk_insert.return_value = 3

        operator = SyncOperator(
            sql_client=mock_sql_client,
            databricks_client=databricks_client,
        )

        import asyncio
        result = asyncio.get_event_loop().run_until_complete(
            operator.process_event(event)
        )

        # Step 6: Verify SQL operations were called correctly
        assert result.success is True
        assert result.rows_affected == 3
        assert result.discrepancy is None

        mock_sql_client.bulk_insert.assert_called_once()
        call_args = mock_sql_client.bulk_insert.call_args
        inserted_df = call_args[0][1]
        assert len(inserted_df) == 3
        assert "id" in inserted_df.columns
        assert "name" in inserted_df.columns

        # Step 7: Update event status in Databricks
        update_event = f"""
        UPDATE {event_table}
        SET status = 'completed',
            rows_affected = {result.rows_affected},
            processed_at = current_timestamp()
        WHERE event_id = '{event_id}'
        """
        databricks_client.execute_sql(update_event)

        # Step 8: Verify event was updated
        final_result = databricks_client.execute_sql(
            f"SELECT status, rows_affected FROM {event_table} WHERE event_id = '{event_id}'"
        )
        assert len(final_result) == 1
        assert final_result[0]["status"] == "completed"
        assert int(final_result[0]["rows_affected"]) == 3

    def test_update_event_flow(
        self, databricks_client, mock_sql_client, setup_test_tables
    ):
        """Test UPDATE event flow: create event → process → verify SQL updates."""
        from sql_databricks_bridge.models.events import SyncEvent, SyncOperation
        from sql_databricks_bridge.sync.operations import SyncOperator

        event_table = setup_test_tables["event_table"]
        source_table = setup_test_tables["source_table"]

        # Step 1: Insert source data (rows to update)
        insert_source = f"""
        INSERT INTO {source_table} VALUES
            (1, 'Alice Updated', 150.0),
            (2, 'Bob Updated', 250.0)
        """
        databricks_client.execute_sql(insert_source)

        # Step 2: Create UPDATE event
        event_id = f"evt-upd-{uuid.uuid4()}"
        insert_event = f"""
        INSERT INTO {event_table} VALUES (
            '{event_id}',
            'UPDATE',
            '{source_table}',
            'dbo.target_table',
            ARRAY('id'),
            'pending',
            2,
            NULL,
            NULL,
            current_timestamp(),
            NULL
        )
        """
        databricks_client.execute_sql(insert_event)

        # Step 3: Create and process event
        event = SyncEvent(
            event_id=event_id,
            operation=SyncOperation.UPDATE,
            source_table=source_table,
            target_table="dbo.target_table",
            primary_keys=["id"],
            rows_expected=2,
        )

        mock_sql_client.execute_write.return_value = 1  # Each UPDATE affects 1 row

        operator = SyncOperator(
            sql_client=mock_sql_client,
            databricks_client=databricks_client,
        )

        import asyncio
        result = asyncio.get_event_loop().run_until_complete(
            operator.process_event(event)
        )

        # Step 4: Verify UPDATE operations
        assert result.success is True
        assert result.rows_affected == 2  # Two UPDATE statements executed
        assert mock_sql_client.execute_write.call_count == 2

        # Verify UPDATE SQL structure
        first_call = mock_sql_client.execute_write.call_args_list[0]
        update_sql = first_call[0][0]
        assert "UPDATE" in update_sql
        assert "[dbo].[target_table]" in update_sql
        assert "SET" in update_sql

        # Step 5: Update event status
        update_event = f"""
        UPDATE {event_table}
        SET status = 'completed',
            rows_affected = {result.rows_affected},
            processed_at = current_timestamp()
        WHERE event_id = '{event_id}'
        """
        databricks_client.execute_sql(update_event)

    def test_delete_event_flow(
        self, databricks_client, mock_sql_client, setup_test_tables
    ):
        """Test DELETE event flow with limit validation."""
        from sql_databricks_bridge.models.events import SyncEvent, SyncOperation
        from sql_databricks_bridge.sync.operations import SyncOperator

        event_table = setup_test_tables["event_table"]
        source_table = setup_test_tables["source_table"]

        # Step 1: Create DELETE event
        event_id = f"evt-del-{uuid.uuid4()}"
        insert_event = f"""
        INSERT INTO {event_table} VALUES (
            '{event_id}',
            'DELETE',
            '{source_table}',
            'dbo.target_table',
            ARRAY('id'),
            'pending',
            5,
            NULL,
            NULL,
            current_timestamp(),
            NULL
        )
        """
        databricks_client.execute_sql(insert_event)

        # Step 2: Create and process event
        event = SyncEvent(
            event_id=event_id,
            operation=SyncOperation.DELETE,
            source_table=source_table,
            target_table="dbo.target_table",
            primary_keys=["id"],
            filter_conditions={"status": "inactive"},
            rows_expected=5,
        )

        # Mock: COUNT returns 5 rows to delete
        mock_sql_client.execute_query.return_value = pl.DataFrame({"cnt": [5]})
        mock_sql_client.execute_write.return_value = 5

        operator = SyncOperator(
            sql_client=mock_sql_client,
            databricks_client=databricks_client,
            max_delete_rows=100,  # Allow up to 100 deletes
        )

        import asyncio
        result = asyncio.get_event_loop().run_until_complete(
            operator.process_event(event)
        )

        # Step 3: Verify DELETE operations
        assert result.success is True
        assert result.rows_affected == 5

        # Verify COUNT was called first
        count_call = mock_sql_client.execute_query.call_args[0][0]
        assert "COUNT" in count_call

        # Verify DELETE was executed
        delete_call = mock_sql_client.execute_write.call_args[0][0]
        assert "DELETE FROM" in delete_call

    def test_failed_event_records_error(
        self, databricks_client, mock_sql_client, setup_test_tables
    ):
        """Test that failed events record error message."""
        from sql_databricks_bridge.models.events import SyncEvent, SyncOperation
        from sql_databricks_bridge.sync.operations import SyncOperator

        event_table = setup_test_tables["event_table"]
        source_table = setup_test_tables["source_table"]

        # Create event
        event_id = f"evt-fail-{uuid.uuid4()}"
        insert_event = f"""
        INSERT INTO {event_table} VALUES (
            '{event_id}',
            'INSERT',
            '{source_table}',
            'dbo.target_table',
            ARRAY('id'),
            'pending',
            NULL,
            NULL,
            NULL,
            current_timestamp(),
            NULL
        )
        """
        databricks_client.execute_sql(insert_event)

        # Make SQL Server fail
        mock_sql_client.bulk_insert.side_effect = Exception("SQL Server connection timeout")

        # Insert some source data
        databricks_client.execute_sql(
            f"INSERT INTO {source_table} VALUES (1, 'Test', 100.0)"
        )

        event = SyncEvent(
            event_id=event_id,
            operation=SyncOperation.INSERT,
            source_table=source_table,
            target_table="dbo.target_table",
        )

        operator = SyncOperator(
            sql_client=mock_sql_client,
            databricks_client=databricks_client,
        )

        import asyncio
        result = asyncio.get_event_loop().run_until_complete(
            operator.process_event(event)
        )

        # Verify failure
        assert result.success is False
        assert "SQL Server connection timeout" in result.error

        # Update event with error
        escaped_error = result.error.replace("'", "''")
        update_event = f"""
        UPDATE {event_table}
        SET status = 'failed',
            error_message = '{escaped_error}',
            processed_at = current_timestamp()
        WHERE event_id = '{event_id}'
        """
        databricks_client.execute_sql(update_event)

        # Verify error was recorded
        final_result = databricks_client.execute_sql(
            f"SELECT status, error_message FROM {event_table} WHERE event_id = '{event_id}'"
        )
        assert final_result[0]["status"] == "failed"
        assert "timeout" in final_result[0]["error_message"]


@requires_databricks
@requires_sql_warehouse
class TestEventTableOperations:
    """Test reading and writing to the event table in Databricks."""

    @pytest.fixture
    def databricks_client(self):
        """Create real Databricks client."""
        from sql_databricks_bridge.db.databricks import DatabricksClient
        return DatabricksClient()

    @pytest.fixture
    def test_event_table(self, databricks_client, request):
        """Create a temporary event table for testing."""
        test_id = str(uuid.uuid4())[:8]
        table_name = f"`{CATALOG}`.`{SCHEMA}`.bridge_events_test_{test_id}"

        create_sql = f"""
        CREATE TABLE {table_name} (
            event_id STRING,
            operation STRING,
            status STRING,
            created_at TIMESTAMP
        )
        """
        databricks_client.execute_sql(create_sql)

        yield table_name

        # Cleanup
        try:
            databricks_client.execute_sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass

    def test_insert_and_read_event(self, databricks_client, test_event_table):
        """Test inserting and reading an event."""
        event_id = str(uuid.uuid4())

        # Insert
        insert_sql = f"""
        INSERT INTO {test_event_table}
        VALUES ('{event_id}', 'INSERT', 'pending', current_timestamp())
        """
        databricks_client.execute_sql(insert_sql)

        # Read
        result = databricks_client.execute_sql(
            f"SELECT * FROM {test_event_table} WHERE event_id = '{event_id}'"
        )

        assert len(result) == 1
        assert result[0]["event_id"] == event_id
        assert result[0]["operation"] == "INSERT"
        assert result[0]["status"] == "pending"

    def test_update_event_status(self, databricks_client, test_event_table):
        """Test updating event status."""
        event_id = str(uuid.uuid4())

        # Insert initial event
        databricks_client.execute_sql(f"""
            INSERT INTO {test_event_table}
            VALUES ('{event_id}', 'UPDATE', 'pending', current_timestamp())
        """)

        # Update status
        databricks_client.execute_sql(f"""
            UPDATE {test_event_table}
            SET status = 'completed'
            WHERE event_id = '{event_id}'
        """)

        # Verify
        result = databricks_client.execute_sql(
            f"SELECT status FROM {test_event_table} WHERE event_id = '{event_id}'"
        )
        assert result[0]["status"] == "completed"

    def test_query_pending_events(self, databricks_client, test_event_table):
        """Test querying only pending events."""
        # Insert multiple events with different statuses
        databricks_client.execute_sql(f"""
            INSERT INTO {test_event_table} VALUES
            ('evt-1', 'INSERT', 'pending', current_timestamp()),
            ('evt-2', 'UPDATE', 'completed', current_timestamp()),
            ('evt-3', 'DELETE', 'pending', current_timestamp()),
            ('evt-4', 'INSERT', 'failed', current_timestamp())
        """)

        # Query pending only
        result = databricks_client.execute_sql(
            f"SELECT * FROM {test_event_table} WHERE status = 'pending' ORDER BY event_id"
        )

        assert len(result) == 2
        assert result[0]["event_id"] == "evt-1"
        assert result[1]["event_id"] == "evt-3"
