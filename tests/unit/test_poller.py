"""Unit tests for EventPoller."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sql_databricks_bridge.models.events import SyncEvent, SyncOperation, SyncStatus
from sql_databricks_bridge.sync.operations import OperationResult
from sql_databricks_bridge.sync.poller import EventPoller


@pytest.fixture
def mock_databricks_client():
    """Create mock Databricks client."""
    mock = MagicMock()
    mock.execute_sql = MagicMock(return_value=[])
    return mock


@pytest.fixture
def mock_sql_client():
    """Create mock SQL Server client."""
    mock = MagicMock()
    return mock


@pytest.fixture
def poller(mock_databricks_client, mock_sql_client):
    """Create EventPoller with mocks."""
    return EventPoller(
        databricks_client=mock_databricks_client,
        sql_client=mock_sql_client,
        events_table="test.schema.events",
        poll_interval=1,
        max_events_per_poll=10,
    )


class TestEventPollerInit:
    """Tests for EventPoller initialization."""

    def test_init_with_defaults(self, mock_databricks_client, mock_sql_client):
        """Initialize with default values."""
        poller = EventPoller(
            databricks_client=mock_databricks_client,
            sql_client=mock_sql_client,
        )

        assert poller.poll_interval == 10
        assert poller.max_events_per_poll == 100
        assert poller.is_running is False

    def test_init_with_custom_values(self, mock_databricks_client, mock_sql_client):
        """Initialize with custom values."""
        poller = EventPoller(
            databricks_client=mock_databricks_client,
            sql_client=mock_sql_client,
            events_table="custom.events",
            poll_interval=5,
            max_events_per_poll=50,
        )

        assert poller.events_table == "custom.events"
        assert poller.poll_interval == 5
        assert poller.max_events_per_poll == 50


class TestEventPollerOperator:
    """Tests for operator property."""

    def test_operator_created_on_access(self, poller):
        """Operator is created lazily."""
        assert poller._operator is None

        operator = poller.operator

        assert operator is not None
        assert poller._operator is operator

    def test_operator_cached(self, poller):
        """Operator is cached after creation."""
        operator1 = poller.operator
        operator2 = poller.operator

        assert operator1 is operator2


class TestQueryPendingEvents:
    """Tests for query_pending_events."""

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_events(self, poller, mock_databricks_client):
        """Returns empty list when no pending events."""
        mock_databricks_client.execute_sql.return_value = []

        events = await poller.query_pending_events()

        assert events == []

    @pytest.mark.asyncio
    async def test_returns_events_from_databricks(self, poller, mock_databricks_client):
        """Returns events from Databricks query."""
        mock_databricks_client.execute_sql.return_value = [
            {
                "event_id": "evt-1",
                "operation": "INSERT",
                "source_table": "cat.sch.src",
                "target_table": "dbo.tgt",
                "primary_keys": ["id"],
                "priority": 1,
                "status": "pending",
                "rows_expected": 100,
                "created_at": None,
                "filter_conditions": None,
                "metadata": None,
            },
        ]

        events = await poller.query_pending_events()

        assert len(events) == 1
        assert events[0].event_id == "evt-1"
        assert events[0].operation == SyncOperation.INSERT

    @pytest.mark.asyncio
    async def test_handles_query_error(self, poller, mock_databricks_client):
        """Returns empty list on query error."""
        mock_databricks_client.execute_sql.side_effect = Exception("Query failed")

        events = await poller.query_pending_events()

        assert events == []

    @pytest.mark.asyncio
    async def test_query_uses_correct_sql(self, poller, mock_databricks_client):
        """Query uses correct table and filters."""
        mock_databricks_client.execute_sql.return_value = []

        await poller.query_pending_events()

        call_args = mock_databricks_client.execute_sql.call_args[0][0]
        assert "test.schema.events" in call_args
        assert "status = 'pending'" in call_args
        assert "ORDER BY priority DESC" in call_args
        assert "LIMIT 10" in call_args


class TestUpdateEventStatus:
    """Tests for update_event_status."""

    @pytest.mark.asyncio
    async def test_updates_status(self, poller, mock_databricks_client):
        """Updates event status in Databricks."""
        event = SyncEvent(
            event_id="evt-1",
            operation=SyncOperation.INSERT,
            source_table="cat.sch.src",
            target_table="dbo.tgt",
        )

        await poller.update_event_status(event, SyncStatus.COMPLETED)

        call_args = mock_databricks_client.execute_sql.call_args[0][0]
        assert "UPDATE test.schema.events" in call_args
        assert "status = 'completed'" in call_args
        assert "event_id = 'evt-1'" in call_args

    @pytest.mark.asyncio
    async def test_updates_with_rows_affected(self, poller, mock_databricks_client):
        """Updates with rows_affected value."""
        event = SyncEvent(
            event_id="evt-1",
            operation=SyncOperation.INSERT,
            source_table="cat.sch.src",
            target_table="dbo.tgt",
        )

        await poller.update_event_status(event, SyncStatus.COMPLETED, rows_affected=50)

        call_args = mock_databricks_client.execute_sql.call_args[0][0]
        assert "rows_affected = 50" in call_args

    @pytest.mark.asyncio
    async def test_updates_with_error_message(self, poller, mock_databricks_client):
        """Updates with error message."""
        event = SyncEvent(
            event_id="evt-1",
            operation=SyncOperation.INSERT,
            source_table="cat.sch.src",
            target_table="dbo.tgt",
        )

        await poller.update_event_status(
            event,
            SyncStatus.FAILED,
            error_message="Connection timeout",
        )

        call_args = mock_databricks_client.execute_sql.call_args[0][0]
        assert "error_message = 'Connection timeout'" in call_args

    @pytest.mark.asyncio
    async def test_escapes_quotes_in_error(self, poller, mock_databricks_client):
        """Escapes single quotes in error message."""
        event = SyncEvent(
            event_id="evt-1",
            operation=SyncOperation.INSERT,
            source_table="cat.sch.src",
            target_table="dbo.tgt",
        )

        await poller.update_event_status(
            event,
            SyncStatus.FAILED,
            error_message="Can't connect",
        )

        call_args = mock_databricks_client.execute_sql.call_args[0][0]
        assert "Can''t connect" in call_args

    @pytest.mark.asyncio
    async def test_handles_update_error(self, poller, mock_databricks_client):
        """Handles update errors gracefully."""
        mock_databricks_client.execute_sql.side_effect = Exception("Update failed")

        event = SyncEvent(
            event_id="evt-1",
            operation=SyncOperation.INSERT,
            source_table="cat.sch.src",
            target_table="dbo.tgt",
        )

        # Should not raise
        await poller.update_event_status(event, SyncStatus.COMPLETED)


class TestProcessEvent:
    """Tests for process_event."""

    @pytest.mark.asyncio
    async def test_processes_event_successfully(self, poller, mock_databricks_client):
        """Processes event and updates status."""
        event = SyncEvent(
            event_id="evt-1",
            operation=SyncOperation.INSERT,
            source_table="cat.sch.src",
            target_table="dbo.tgt",
        )

        mock_result = OperationResult(success=True, rows_affected=100)

        # Create a mock operator and assign it directly
        mock_operator = MagicMock()
        mock_operator.process_event = AsyncMock(return_value=mock_result)
        poller._operator = mock_operator

        with patch(
            "sql_databricks_bridge.sync.poller.retry_async",
            new=AsyncMock(return_value=mock_result),
        ):
            await poller.process_event(event)

        # Should have updated status twice (processing, then completed)
        assert mock_databricks_client.execute_sql.call_count >= 1

    @pytest.mark.asyncio
    async def test_calls_event_callback(self, poller, mock_databricks_client):
        """Calls event callback after processing."""
        callback = MagicMock()
        poller.set_event_callback(callback)

        event = SyncEvent(
            event_id="evt-1",
            operation=SyncOperation.INSERT,
            source_table="cat.sch.src",
            target_table="dbo.tgt",
        )

        mock_result = OperationResult(success=True, rows_affected=100)

        with patch(
            "sql_databricks_bridge.sync.poller.retry_async",
            new=AsyncMock(return_value=mock_result),
        ):
            await poller.process_event(event)

        callback.assert_called_once_with(event)


class TestPollCycle:
    """Tests for poll_cycle."""

    @pytest.mark.asyncio
    async def test_returns_zero_when_no_events(self, poller, mock_databricks_client):
        """Returns 0 when no pending events."""
        mock_databricks_client.execute_sql.return_value = []

        count = await poller.poll_cycle()

        assert count == 0

    @pytest.mark.asyncio
    async def test_processes_all_pending_events(self, poller, mock_databricks_client):
        """Processes all pending events."""
        mock_databricks_client.execute_sql.return_value = [
            {
                "event_id": "evt-1",
                "operation": "INSERT",
                "source_table": "cat.sch.src",
                "target_table": "dbo.tgt",
                "primary_keys": [],
                "priority": 0,
                "status": "pending",
                "rows_expected": None,
                "created_at": None,
                "filter_conditions": None,
                "metadata": None,
            },
            {
                "event_id": "evt-2",
                "operation": "UPDATE",
                "source_table": "cat.sch.src",
                "target_table": "dbo.tgt",
                "primary_keys": ["id"],
                "priority": 0,
                "status": "pending",
                "rows_expected": None,
                "created_at": None,
                "filter_conditions": None,
                "metadata": None,
            },
        ]

        mock_result = OperationResult(success=True, rows_affected=10)

        with patch(
            "sql_databricks_bridge.sync.poller.retry_async",
            new=AsyncMock(return_value=mock_result),
        ):
            count = await poller.poll_cycle()

        assert count == 2


class TestPollerStartStop:
    """Tests for start/stop functionality."""

    def test_is_running_initially_false(self, poller):
        """Poller is not running initially."""
        assert poller.is_running is False

    def test_stop_sets_running_false(self, poller):
        """Stop sets running to False."""
        poller._running = True

        poller.stop()

        assert poller.is_running is False

    def test_set_event_callback(self, poller):
        """Set event callback stores callback."""
        callback = MagicMock()

        poller.set_event_callback(callback)

        assert poller._on_event_processed is callback
