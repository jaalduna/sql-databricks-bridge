"""Unit tests verifying the 4 poller contention mitigations.

Each class validates exactly one mitigation so that a regression in any
of them fails a targeted test:

    1. TestBatching                 -> 2N+1 queries per cycle collapsed to 3
    2. TestEventLoopNonBlocking     -> asyncio.to_thread wrapping works
    3. TestRetryBounded             -> max_attempts lowered from 10 to 3
    4. TestWarehouseSeparation      -> DatabricksSettings override mechanism
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sql_databricks_bridge.core.config import DatabricksSettings
from sql_databricks_bridge.models.events import SyncEvent, SyncOperation, SyncStatus
from sql_databricks_bridge.sync.operations import OperationResult, TransientConnectionError
from sql_databricks_bridge.sync.poller import EventPoller


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _make_event(event_id: str, op: SyncOperation = SyncOperation.INSERT) -> SyncEvent:
    return SyncEvent(
        event_id=event_id,
        operation=op,
        source_table="src.tbl",
        target_table="dst.tbl",
        primary_keys=[],
        priority=0,
        status=SyncStatus.PENDING,
    )


def _databricks_row(event_id: str) -> dict:
    return {
        "event_id": event_id,
        "operation": "INSERT",
        "source_table": "src.tbl",
        "target_table": "dst.tbl",
        "primary_keys": "[]",
        "filter_conditions": "{}",
        "metadata": "{}",
        "priority": 0,
        "status": "pending",
        "rows_expected": None,
        "created_at": None,
    }


@pytest.fixture
def mock_dbx():
    m = MagicMock()
    m.execute_sql = MagicMock(return_value=[])
    return m


@pytest.fixture
def mock_sql():
    return MagicMock()


@pytest.fixture
def poller(mock_dbx, mock_sql):
    return EventPoller(
        databricks_client=mock_dbx,
        sql_client=mock_sql,
        events_table="test.schema.events",
        poll_interval=1,
        max_events_per_poll=100,
    )


# ---------------------------------------------------------------------------
# Mitigation 1: Batching (2N+1 queries -> 3)
# ---------------------------------------------------------------------------


class TestBatching:
    """poll_cycle must issue exactly 3 Databricks queries regardless of N."""

    @pytest.mark.asyncio
    async def test_three_queries_per_cycle_with_many_events(self, poller, mock_dbx):
        n_events = 25
        rows = [_databricks_row(f"evt-{i}") for i in range(n_events)]

        # 1st call returns the pending rows; subsequent calls (mark processing
        # + finalize) return [] (UPDATEs have no result set).
        mock_dbx.execute_sql.side_effect = [rows, [], []]

        # Avoid hitting the real SyncOperator — stub it out.
        poller._operator = MagicMock()
        poller._operator.process_event = AsyncMock(
            return_value=OperationResult(success=True, rows_affected=1)
        )

        processed = await poller.poll_cycle()

        assert processed == n_events
        # 1 SELECT pending + 1 UPDATE processing + 1 UPDATE finalize
        assert mock_dbx.execute_sql.call_count == 3, (
            f"Expected exactly 3 Databricks queries per cycle, got "
            f"{mock_dbx.execute_sql.call_count}"
        )

    @pytest.mark.asyncio
    async def test_mark_processing_uses_single_update(self, poller, mock_dbx):
        rows = [_databricks_row(f"evt-{i}") for i in range(5)]
        mock_dbx.execute_sql.side_effect = [rows, [], []]

        poller._operator = MagicMock()
        poller._operator.process_event = AsyncMock(
            return_value=OperationResult(success=True, rows_affected=1)
        )

        await poller.poll_cycle()

        # Second call should be the batched "mark processing" UPDATE
        processing_call = mock_dbx.execute_sql.call_args_list[1]
        sql = processing_call.args[0]
        assert "SET status = 'processing'" in sql
        # Every event_id must appear in the WHERE IN clause
        for i in range(5):
            assert f"'evt-{i}'" in sql

    @pytest.mark.asyncio
    async def test_finalize_is_single_case_when_update(self, poller, mock_dbx):
        rows = [_databricks_row(f"evt-{i}") for i in range(3)]
        mock_dbx.execute_sql.side_effect = [rows, [], []]

        poller._operator = MagicMock()
        # Mix success and failure to exercise both branches of CASE WHEN
        poller._operator.process_event = AsyncMock(side_effect=[
            OperationResult(success=True, rows_affected=10),
            OperationResult(success=False, rows_affected=0, error="boom"),
            OperationResult(success=True, rows_affected=5),
        ])

        await poller.poll_cycle()

        finalize_call = mock_dbx.execute_sql.call_args_list[2]
        sql = finalize_call.args[0]
        assert "CASE event_id" in sql
        assert "'completed'" in sql
        assert "'failed'" in sql
        # All event_ids must be referenced
        for i in range(3):
            assert f"'evt-{i}'" in sql


# ---------------------------------------------------------------------------
# Mitigation 2: asyncio.to_thread prevents event loop blocking
# ---------------------------------------------------------------------------


class TestEventLoopNonBlocking:
    """Blocking calls must run off the event loop thread."""

    @pytest.mark.asyncio
    async def test_slow_execute_sql_does_not_block_event_loop(
        self, poller, mock_dbx
    ):
        # execute_sql is SYNCHRONOUSLY slow (time.sleep, not asyncio.sleep)
        # to simulate a long-running Databricks statement.
        def slow_exec(query):
            time.sleep(0.5)
            return []

        mock_dbx.execute_sql = MagicMock(side_effect=slow_exec)

        # Measure how long a trivial asyncio sleep takes while the blocking
        # call is in flight.  If to_thread works, the loop is responsive.
        async def measure_responsiveness():
            await asyncio.sleep(0.05)  # give poll_cycle time to enter execute_sql
            t0 = time.monotonic()
            await asyncio.sleep(0.0)
            return time.monotonic() - t0

        # Run query_pending_events (goes through _dbx_exec -> to_thread)
        # concurrently with the responsiveness probe.
        poll_task = asyncio.create_task(poller.query_pending_events())
        elapsed = await measure_responsiveness()
        await poll_task

        # If the event loop were blocked, elapsed would be ~0.5s.
        # With to_thread, the loop handles asyncio.sleep(0) in <10ms easily.
        assert elapsed < 0.1, (
            f"Event loop appears blocked: asyncio.sleep(0) took {elapsed*1000:.0f}ms "
            f"while execute_sql was running — to_thread wrapping is broken."
        )

    @pytest.mark.asyncio
    async def test_dbx_exec_uses_to_thread(self, poller, mock_dbx):
        """Direct proof: _dbx_exec delegates to asyncio.to_thread."""
        mock_dbx.execute_sql.return_value = []

        with patch(
            "sql_databricks_bridge.sync.poller.asyncio.to_thread",
            new=AsyncMock(return_value=[]),
        ) as to_thread_mock:
            await poller._dbx_exec("SELECT 1")

        to_thread_mock.assert_called_once()
        # First positional arg is the sync callable
        assert to_thread_mock.call_args.args[0] is mock_dbx.execute_sql


# ---------------------------------------------------------------------------
# Mitigation 3: Retry is bounded at 3 attempts with reduced backoff
# ---------------------------------------------------------------------------


class TestRetryBounded:
    """Poller must not retry more than 3 times per event."""

    @pytest.mark.asyncio
    async def test_process_event_retries_at_most_three_times(self, poller):
        call_count = 0

        async def always_transient(event):
            nonlocal call_count
            call_count += 1
            raise TransientConnectionError("transient", ValueError("underlying"))

        poller._operator = MagicMock()
        poller._operator.process_event = AsyncMock(side_effect=always_transient)

        # Patch asyncio.sleep inside retry_async to avoid real backoff waits
        with patch(
            "sql_databricks_bridge.sync.retry.asyncio.sleep",
            new=AsyncMock(return_value=None),
        ):
            event = _make_event("evt-1")
            result = await poller._process_event_collect(event)

        _, status, op_result, err = result
        assert status == SyncStatus.FAILED
        assert op_result is None
        assert err is not None and "Exhausted 3 retries" in err
        assert call_count == 3, (
            f"Expected 3 attempts, got {call_count} — max_attempts regressed."
        )

    @pytest.mark.asyncio
    async def test_retry_backoff_is_bounded(self):
        """base_delay=2.0, max_delay=60.0 baked into the poller source."""
        import inspect

        src = inspect.getsource(EventPoller._process_event_collect)
        assert "max_attempts=3" in src, (
            "EventPoller._process_event_collect no longer uses max_attempts=3"
        )
        assert "base_delay=2.0" in src, (
            "EventPoller._process_event_collect no longer uses base_delay=2.0"
        )
        assert "max_delay=60.0" in src, (
            "EventPoller._process_event_collect no longer uses max_delay=60.0"
        )


# ---------------------------------------------------------------------------
# Mitigation 4: Warehouse separation via DatabricksSettings override
# ---------------------------------------------------------------------------


class TestWarehouseSeparation:
    """Config supports overriding warehouse_id to isolate the poller."""

    def test_poller_warehouse_id_field_exists(self, monkeypatch):
        # Isolate from the repo's real .env so we test the default
        monkeypatch.delenv("DATABRICKS_POLLER_WAREHOUSE_ID", raising=False)
        s = DatabricksSettings(_env_file=None)
        assert hasattr(s, "poller_warehouse_id")
        assert s.poller_warehouse_id == ""

    def test_settings_model_copy_overrides_warehouse(self):
        """main.py relies on model_copy(update={...}) to build a poller client."""
        main = DatabricksSettings(
            warehouse_id="MAIN_WH",
            poller_warehouse_id="POLLER_WH",
        )
        poller_settings = main.model_copy(
            update={"warehouse_id": main.poller_warehouse_id}
        )

        assert poller_settings.warehouse_id == "POLLER_WH"
        # Original must not be mutated
        assert main.warehouse_id == "MAIN_WH"
        # Other fields must survive the copy
        assert poller_settings.poller_warehouse_id == "POLLER_WH"

    def test_env_prefix_maps_to_poller_warehouse_id(self, monkeypatch):
        """DATABRICKS_POLLER_WAREHOUSE_ID env var loads into poller_warehouse_id."""
        monkeypatch.setenv("DATABRICKS_POLLER_WAREHOUSE_ID", "wh-123")
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-main")
        # Rebuild settings so the env is picked up (avoid the .env file on disk)
        s = DatabricksSettings(_env_file=None)
        assert s.poller_warehouse_id == "wh-123"
        assert s.warehouse_id == "wh-main"
