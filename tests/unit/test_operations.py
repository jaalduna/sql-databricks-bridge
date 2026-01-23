"""Unit tests for sync operations."""

from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from sql_databricks_bridge.models.events import SyncEvent, SyncOperation, SyncStatus
from sql_databricks_bridge.sync.operations import OperationResult, SyncOperator
from sql_databricks_bridge.sync.validators import ValidationError


class TestSyncEvent:
    """Tests for SyncEvent model."""

    def test_can_retry_true(self):
        """Event can be retried when under max retries."""
        event = SyncEvent(
            event_id="1",
            operation=SyncOperation.INSERT,
            source_table="cat.sch.table",
            target_table="dbo.table",
            retry_count=2,
            max_retries=3,
        )

        assert event.can_retry is True

    def test_can_retry_false(self):
        """Event cannot be retried when at max retries."""
        event = SyncEvent(
            event_id="1",
            operation=SyncOperation.INSERT,
            source_table="cat.sch.table",
            target_table="dbo.table",
            retry_count=3,
            max_retries=3,
        )

        assert event.can_retry is False

    def test_has_discrepancy_true(self):
        """Discrepancy detected when expected != affected."""
        event = SyncEvent(
            event_id="1",
            operation=SyncOperation.INSERT,
            source_table="cat.sch.table",
            target_table="dbo.table",
            rows_expected=100,
            rows_affected=95,
        )

        assert event.has_discrepancy is True

    def test_has_discrepancy_false(self):
        """No discrepancy when expected == affected."""
        event = SyncEvent(
            event_id="1",
            operation=SyncOperation.INSERT,
            source_table="cat.sch.table",
            target_table="dbo.table",
            rows_expected=100,
            rows_affected=100,
        )

        assert event.has_discrepancy is False

    def test_has_discrepancy_missing_values(self):
        """No discrepancy when values not set."""
        event = SyncEvent(
            event_id="1",
            operation=SyncOperation.INSERT,
            source_table="cat.sch.table",
            target_table="dbo.table",
        )

        assert event.has_discrepancy is False


class TestOperationResult:
    """Tests for OperationResult."""

    def test_success_result(self):
        """Successful operation result."""
        result = OperationResult(
            success=True,
            rows_affected=100,
        )

        assert result.success is True
        assert result.rows_affected == 100
        assert result.error is None

    def test_failure_result(self):
        """Failed operation result."""
        result = OperationResult(
            success=False,
            rows_affected=0,
            error="Connection failed",
        )

        assert result.success is False
        assert result.error == "Connection failed"

    def test_result_with_discrepancy(self):
        """Result with discrepancy warning."""
        result = OperationResult(
            success=True,
            rows_affected=95,
            discrepancy=5,
            warning="5 rows less than expected",
        )

        assert result.discrepancy == 5
        assert result.warning is not None


class TestSyncStatus:
    """Tests for SyncStatus enum."""

    def test_all_statuses(self):
        """All status values exist."""
        assert SyncStatus.PENDING.value == "pending"
        assert SyncStatus.PROCESSING.value == "processing"
        assert SyncStatus.COMPLETED.value == "completed"
        assert SyncStatus.FAILED.value == "failed"
        assert SyncStatus.BLOCKED.value == "blocked"
        assert SyncStatus.RETRY.value == "retry"


class TestSyncOperation:
    """Tests for SyncOperation enum."""

    def test_all_operations(self):
        """All operation values exist."""
        assert SyncOperation.INSERT.value == "INSERT"
        assert SyncOperation.UPDATE.value == "UPDATE"
        assert SyncOperation.DELETE.value == "DELETE"


# --- Fixtures for SyncOperator tests ---


@pytest.fixture
def mock_sql_client():
    """Create a mock SQL Server client."""
    client = MagicMock()
    client.bulk_insert = MagicMock(return_value=100)
    client.execute_write = MagicMock(return_value=1)
    client.execute_query = MagicMock(return_value=pl.DataFrame({"cnt": [5]}))
    return client


@pytest.fixture
def mock_databricks_client():
    """Create a mock Databricks client."""
    client = MagicMock()
    client.execute_sql = MagicMock(
        return_value=[
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 2, "name": "Bob", "value": 200},
        ]
    )
    return client


@pytest.fixture
def sync_operator(mock_sql_client, mock_databricks_client):
    """Create a SyncOperator with mocked clients."""
    return SyncOperator(
        sql_client=mock_sql_client,
        databricks_client=mock_databricks_client,
    )


# --- Tests for SyncOperator ---


class TestSyncOperatorInsert:
    """Tests for INSERT operations (Databricks → SQL Server)."""

    @pytest.mark.asyncio
    async def test_insert_success(self, sync_operator, mock_sql_client, mock_databricks_client):
        """Successful INSERT reads from Databricks and bulk inserts to SQL."""
        event = SyncEvent(
            event_id="evt-001",
            operation=SyncOperation.INSERT,
            source_table="catalog.schema.source_table",
            target_table="dbo.target_table",
        )

        result = await sync_operator.process_event(event)

        assert result.success is True
        assert result.rows_affected == 100
        assert result.error is None

        # Verify Databricks was queried
        mock_databricks_client.execute_sql.assert_called_once()
        call_args = mock_databricks_client.execute_sql.call_args[0][0]
        assert "catalog.schema.source_table" in call_args

        # Verify SQL bulk insert was called
        mock_sql_client.bulk_insert.assert_called_once()
        call_args = mock_sql_client.bulk_insert.call_args
        assert call_args[0][0] == "target_table"
        assert call_args[1]["schema"] == "dbo"

    @pytest.mark.asyncio
    async def test_insert_empty_data(self, sync_operator, mock_databricks_client):
        """INSERT with no data returns success with warning."""
        mock_databricks_client.execute_sql.return_value = []

        event = SyncEvent(
            event_id="evt-002",
            operation=SyncOperation.INSERT,
            source_table="catalog.schema.empty_table",
            target_table="dbo.target_table",
        )

        result = await sync_operator.process_event(event)

        assert result.success is True
        assert result.rows_affected == 0
        assert result.warning == "No data to insert"

    @pytest.mark.asyncio
    async def test_insert_with_discrepancy(self, sync_operator, mock_sql_client):
        """INSERT reports discrepancy when rows_expected != rows_affected."""
        mock_sql_client.bulk_insert.return_value = 95

        event = SyncEvent(
            event_id="evt-003",
            operation=SyncOperation.INSERT,
            source_table="catalog.schema.table",
            target_table="dbo.target_table",
            rows_expected=100,
        )

        result = await sync_operator.process_event(event)

        assert result.success is True
        assert result.rows_affected == 95
        assert result.discrepancy == 5  # 100 - 95

    @pytest.mark.asyncio
    async def test_insert_with_primary_keys_validates(
        self, sync_operator, mock_databricks_client
    ):
        """INSERT validates primary keys exist in data when specified."""
        # Data missing required PK column
        mock_databricks_client.execute_sql.return_value = [
            {"name": "Alice"},
            {"name": "Bob"},
        ]

        event = SyncEvent(
            event_id="evt-004",
            operation=SyncOperation.INSERT,
            source_table="catalog.schema.table",
            target_table="dbo.target_table",
            primary_keys=["id"],  # 'id' not in data
        )

        result = await sync_operator.process_event(event)

        assert result.success is False
        assert "Missing primary key columns" in result.error

    @pytest.mark.asyncio
    async def test_insert_detects_duplicate_pks(
        self, sync_operator, mock_sql_client, mock_databricks_client
    ):
        """INSERT warns about duplicate primary keys."""
        mock_databricks_client.execute_sql.return_value = [
            {"id": 1, "name": "Alice"},
            {"id": 1, "name": "Alice2"},  # Duplicate PK
            {"id": 2, "name": "Bob"},
        ]

        event = SyncEvent(
            event_id="evt-005",
            operation=SyncOperation.INSERT,
            source_table="catalog.schema.table",
            target_table="dbo.target_table",
            primary_keys=["id"],
        )

        result = await sync_operator.process_event(event)

        assert result.success is True
        assert result.warning is not None
        assert "duplicate" in result.warning.lower()


class TestSyncOperatorUpdate:
    """Tests for UPDATE operations (Databricks → SQL Server)."""

    @pytest.mark.asyncio
    async def test_update_success(self, sync_operator, mock_sql_client, mock_databricks_client):
        """Successful UPDATE reads from Databricks and updates SQL rows."""
        mock_databricks_client.execute_sql.return_value = [
            {"id": 1, "name": "Alice Updated", "value": 150},
        ]

        event = SyncEvent(
            event_id="evt-010",
            operation=SyncOperation.UPDATE,
            source_table="catalog.schema.updates",
            target_table="dbo.target_table",
            primary_keys=["id"],
        )

        result = await sync_operator.process_event(event)

        assert result.success is True
        assert result.rows_affected == 1

        # Verify UPDATE query was executed
        mock_sql_client.execute_write.assert_called_once()
        call_args = mock_sql_client.execute_write.call_args[0][0]
        assert "UPDATE" in call_args
        assert "[dbo].[target_table]" in call_args

    @pytest.mark.asyncio
    async def test_update_multiple_rows(self, sync_operator, mock_sql_client, mock_databricks_client):
        """UPDATE processes multiple rows."""
        mock_databricks_client.execute_sql.return_value = [
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 2, "name": "Bob", "value": 200},
            {"id": 3, "name": "Charlie", "value": 300},
        ]
        mock_sql_client.execute_write.return_value = 1

        event = SyncEvent(
            event_id="evt-011",
            operation=SyncOperation.UPDATE,
            source_table="catalog.schema.updates",
            target_table="dbo.target_table",
            primary_keys=["id"],
        )

        result = await sync_operator.process_event(event)

        assert result.success is True
        assert result.rows_affected == 3
        assert mock_sql_client.execute_write.call_count == 3

    @pytest.mark.asyncio
    async def test_update_empty_data(self, sync_operator, mock_databricks_client):
        """UPDATE with no data returns success with warning."""
        mock_databricks_client.execute_sql.return_value = []

        event = SyncEvent(
            event_id="evt-012",
            operation=SyncOperation.UPDATE,
            source_table="catalog.schema.empty_updates",
            target_table="dbo.target_table",
            primary_keys=["id"],
        )

        result = await sync_operator.process_event(event)

        assert result.success is True
        assert result.rows_affected == 0
        assert result.warning == "No data to update"

    @pytest.mark.asyncio
    async def test_update_requires_primary_keys(self, sync_operator):
        """UPDATE fails validation without primary keys."""
        event = SyncEvent(
            event_id="evt-013",
            operation=SyncOperation.UPDATE,
            source_table="catalog.schema.table",
            target_table="dbo.target_table",
            primary_keys=[],  # Missing PKs
        )

        result = await sync_operator.process_event(event)

        assert result.success is False
        assert "Primary keys required" in result.error

    @pytest.mark.asyncio
    async def test_update_with_discrepancy(self, sync_operator, mock_sql_client, mock_databricks_client):
        """UPDATE reports discrepancy when some rows don't match."""
        mock_databricks_client.execute_sql.return_value = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        # First update succeeds, second doesn't match any rows
        mock_sql_client.execute_write.side_effect = [1, 0]

        event = SyncEvent(
            event_id="evt-014",
            operation=SyncOperation.UPDATE,
            source_table="catalog.schema.updates",
            target_table="dbo.target_table",
            primary_keys=["id"],
            rows_expected=2,
        )

        result = await sync_operator.process_event(event)

        assert result.success is True
        assert result.rows_affected == 1
        assert result.discrepancy == 1  # Expected 2, affected 1


class TestSyncOperatorDelete:
    """Tests for DELETE operations (Databricks → SQL Server)."""

    @pytest.mark.asyncio
    async def test_delete_success(self, sync_operator, mock_sql_client):
        """Successful DELETE removes rows from SQL Server."""
        mock_sql_client.execute_query.return_value = pl.DataFrame({"cnt": [5]})
        mock_sql_client.execute_write.return_value = 5

        event = SyncEvent(
            event_id="evt-020",
            operation=SyncOperation.DELETE,
            source_table="catalog.schema.table",
            target_table="dbo.target_table",
            primary_keys=["id"],
            filter_conditions={"id": 123},
        )

        result = await sync_operator.process_event(event)

        assert result.success is True
        assert result.rows_affected == 5

        # Verify DELETE query was executed
        delete_call = mock_sql_client.execute_write.call_args[0][0]
        assert "DELETE FROM" in delete_call
        assert "[dbo].[target_table]" in delete_call

    @pytest.mark.asyncio
    async def test_delete_requires_primary_keys(self, sync_operator):
        """DELETE fails validation without primary keys."""
        event = SyncEvent(
            event_id="evt-021",
            operation=SyncOperation.DELETE,
            source_table="catalog.schema.table",
            target_table="dbo.target_table",
            primary_keys=[],  # Missing PKs
        )

        result = await sync_operator.process_event(event)

        assert result.success is False
        assert "Primary keys required" in result.error

    @pytest.mark.asyncio
    async def test_delete_with_limit(self, mock_sql_client, mock_databricks_client):
        """DELETE respects max_delete_rows limit."""
        mock_sql_client.execute_query.return_value = pl.DataFrame({"cnt": [1000]})

        operator = SyncOperator(
            sql_client=mock_sql_client,
            databricks_client=mock_databricks_client,
            max_delete_rows=100,  # Limit to 100 rows
        )

        event = SyncEvent(
            event_id="evt-022",
            operation=SyncOperation.DELETE,
            source_table="catalog.schema.table",
            target_table="dbo.target_table",
            primary_keys=["id"],
            filter_conditions={"status": "old"},
        )

        result = await operator.process_event(event)

        assert result.success is False
        assert "DELETE limit exceeded" in result.error
        assert "1000 > 100" in result.error

    @pytest.mark.asyncio
    async def test_delete_within_limit(self, mock_sql_client, mock_databricks_client):
        """DELETE proceeds when within limit."""
        mock_sql_client.execute_query.return_value = pl.DataFrame({"cnt": [50]})
        mock_sql_client.execute_write.return_value = 50

        operator = SyncOperator(
            sql_client=mock_sql_client,
            databricks_client=mock_databricks_client,
            max_delete_rows=100,
        )

        event = SyncEvent(
            event_id="evt-023",
            operation=SyncOperation.DELETE,
            source_table="catalog.schema.table",
            target_table="dbo.target_table",
            primary_keys=["id"],
            filter_conditions={"status": "old"},
        )

        result = await operator.process_event(event)

        assert result.success is True
        assert result.rows_affected == 50

    @pytest.mark.asyncio
    async def test_delete_with_discrepancy(self, sync_operator, mock_sql_client):
        """DELETE reports discrepancy when rows_expected != rows_affected."""
        mock_sql_client.execute_query.return_value = pl.DataFrame({"cnt": [10]})
        mock_sql_client.execute_write.return_value = 8  # Only deleted 8

        event = SyncEvent(
            event_id="evt-024",
            operation=SyncOperation.DELETE,
            source_table="catalog.schema.table",
            target_table="dbo.target_table",
            primary_keys=["id"],
            filter_conditions={"id": 1},
            rows_expected=10,
        )

        result = await sync_operator.process_event(event)

        assert result.success is True
        assert result.rows_affected == 8
        assert result.discrepancy == 2


class TestSyncOperatorErrorHandling:
    """Tests for error handling in SyncOperator."""

    @pytest.mark.asyncio
    async def test_databricks_error_handled(self, sync_operator, mock_databricks_client):
        """Errors from Databricks are caught and reported."""
        mock_databricks_client.execute_sql.side_effect = Exception("Databricks connection failed")

        event = SyncEvent(
            event_id="evt-030",
            operation=SyncOperation.INSERT,
            source_table="catalog.schema.table",
            target_table="dbo.target_table",
        )

        result = await sync_operator.process_event(event)

        assert result.success is False
        assert "Databricks connection failed" in result.error
        assert result.rows_affected == 0

    @pytest.mark.asyncio
    async def test_sql_server_error_handled(self, sync_operator, mock_sql_client, mock_databricks_client):
        """Errors from SQL Server are caught and reported."""
        mock_sql_client.bulk_insert.side_effect = Exception("SQL Server timeout")

        event = SyncEvent(
            event_id="evt-031",
            operation=SyncOperation.INSERT,
            source_table="catalog.schema.table",
            target_table="dbo.target_table",
        )

        result = await sync_operator.process_event(event)

        assert result.success is False
        assert "SQL Server timeout" in result.error

    @pytest.mark.asyncio
    async def test_invalid_source_table_format(self, sync_operator):
        """Invalid Databricks table format causes validation error."""
        event = SyncEvent(
            event_id="evt-032",
            operation=SyncOperation.INSERT,
            source_table="invalid_table",  # Should be catalog.schema.table
            target_table="dbo.target_table",
        )

        result = await sync_operator.process_event(event)

        assert result.success is False
        assert "Invalid Databricks table format" in result.error

    @pytest.mark.asyncio
    async def test_duration_tracked(self, sync_operator):
        """Operation duration is tracked."""
        event = SyncEvent(
            event_id="evt-033",
            operation=SyncOperation.INSERT,
            source_table="catalog.schema.table",
            target_table="dbo.target_table",
        )

        result = await sync_operator.process_event(event)

        assert result.duration_seconds >= 0


class TestSyncOperatorReadFromDatabricks:
    """Tests for _read_from_databricks helper."""

    @pytest.mark.asyncio
    async def test_read_returns_dataframe(self, sync_operator, mock_databricks_client):
        """Data from Databricks is returned as Polars DataFrame."""
        mock_databricks_client.execute_sql.return_value = [
            {"col1": "a", "col2": 1},
            {"col1": "b", "col2": 2},
        ]

        df = await sync_operator._read_from_databricks("SELECT * FROM table")

        assert isinstance(df, pl.DataFrame)
        assert len(df) == 2
        assert df.columns == ["col1", "col2"]

    @pytest.mark.asyncio
    async def test_read_empty_returns_empty_dataframe(self, sync_operator, mock_databricks_client):
        """Empty result returns empty DataFrame."""
        mock_databricks_client.execute_sql.return_value = []

        df = await sync_operator._read_from_databricks("SELECT * FROM empty")

        assert isinstance(df, pl.DataFrame)
        assert df.is_empty()
