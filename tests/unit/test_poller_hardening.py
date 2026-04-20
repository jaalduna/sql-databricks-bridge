"""Unit tests for the 5 poller hardening fixes (R2, S2, R4, S3, P1).

These tests are written TDD-style — they fail against the current
implementation and pass once each fix is in.

    R2  poll_cycle aborts cleanly if mark-processing fails
    S2  batch failure logs include the involved event_ids
    R4  long warning/error_message are truncated before being written
    S3  startup validates the poller warehouse exists
    P1  concurrent poll_cycle calls skip instead of running twice
"""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sql_databricks_bridge.models.events import SyncEvent, SyncOperation, SyncStatus
from sql_databricks_bridge.sync.operations import OperationResult
from sql_databricks_bridge.sync.poller import EventPoller


# ---------------------------------------------------------------------------
# Fixtures (subset replicated from test_poller_mitigations)
# ---------------------------------------------------------------------------


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


def _make_event(event_id: str) -> SyncEvent:
    return SyncEvent(
        event_id=event_id,
        operation=SyncOperation.INSERT,
        source_table="src.tbl",
        target_table="dst.tbl",
        primary_keys=[],
        priority=0,
        status=SyncStatus.PENDING,
    )


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
# R2 — poll_cycle aborts if _batch_mark_processing fails
# ---------------------------------------------------------------------------


class TestR2AbortOnMarkFailure:
    """If marking events as 'processing' fails, poll_cycle must NOT execute
    the work (events stay pending, scheduler retries next tick)."""

    @pytest.mark.asyncio
    async def test_poll_cycle_aborts_when_mark_processing_fails(
        self, poller, mock_dbx
    ):
        # SELECT returns 3 events; next execute_sql call (the mark) will raise.
        rows = [_databricks_row(f"evt-{i}") for i in range(3)]

        call_count = {"n": 0}

        def side(query, *args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return rows  # SELECT pending
            raise RuntimeError("Databricks unavailable")  # the UPDATE mark

        mock_dbx.execute_sql = MagicMock(side_effect=side)

        operator_mock = MagicMock()
        operator_mock.process_event = AsyncMock(
            return_value=OperationResult(success=True, rows_affected=1)
        )
        poller._operator = operator_mock

        processed = await poller.poll_cycle()

        assert processed == 0, "Cycle must return 0 on mark failure"
        # Events must NOT have been processed in SQL Server
        assert operator_mock.process_event.call_count == 0, (
            "No events should be processed when mark-processing failed — "
            "this protects against duplicate execution on next cycle"
        )

    @pytest.mark.asyncio
    async def test_poll_cycle_does_not_finalize_when_mark_fails(
        self, poller, mock_dbx
    ):
        rows = [_databricks_row("e1"), _databricks_row("e2")]
        mock_dbx.execute_sql = MagicMock(side_effect=[rows, RuntimeError("boom")])

        operator_mock = MagicMock()
        operator_mock.process_event = AsyncMock(
            return_value=OperationResult(success=True, rows_affected=1)
        )
        poller._operator = operator_mock

        await poller.poll_cycle()

        # Only 2 calls: SELECT + failed mark.  NO finalize UPDATE issued.
        assert mock_dbx.execute_sql.call_count == 2


# ---------------------------------------------------------------------------
# S2 — batch failure logs must include event_ids
# ---------------------------------------------------------------------------


class TestS2ErrorLogsIncludeEventIds:
    @pytest.mark.asyncio
    async def test_mark_processing_failure_logs_event_ids(
        self, poller, mock_dbx, caplog
    ):
        events = [_make_event(f"evt-{i}") for i in range(3)]
        mock_dbx.execute_sql = MagicMock(side_effect=RuntimeError("boom"))

        with caplog.at_level(logging.ERROR, logger="sql_databricks_bridge.sync.poller"):
            try:
                await poller._batch_mark_processing(events)
            except Exception:
                pass  # R2 may make this raise — we still want the log

        combined = " ".join(r.getMessage() for r in caplog.records)
        for i in range(3):
            assert f"evt-{i}" in combined, (
                f"event_id evt-{i} must appear in the error log to allow forensics"
            )

    @pytest.mark.asyncio
    async def test_finalize_failure_logs_event_ids(
        self, poller, mock_dbx, caplog
    ):
        results = [
            (_make_event("fail-1"), SyncStatus.COMPLETED,
             OperationResult(success=True, rows_affected=1), None),
            (_make_event("fail-2"), SyncStatus.FAILED, None, "boom"),
        ]
        mock_dbx.execute_sql = MagicMock(side_effect=RuntimeError("db down"))

        with caplog.at_level(logging.ERROR, logger="sql_databricks_bridge.sync.poller"):
            await poller._batch_finalize(results)

        combined = " ".join(r.getMessage() for r in caplog.records)
        assert "fail-1" in combined
        assert "fail-2" in combined

    @pytest.mark.asyncio
    async def test_log_truncates_event_id_list_when_many(
        self, poller, mock_dbx, caplog
    ):
        # 50 events — log must not print all of them verbatim
        events = [_make_event(f"bulk-{i}") for i in range(50)]
        mock_dbx.execute_sql = MagicMock(side_effect=RuntimeError("boom"))

        with caplog.at_level(logging.ERROR, logger="sql_databricks_bridge.sync.poller"):
            try:
                await poller._batch_mark_processing(events)
            except Exception:
                pass

        combined = " ".join(r.getMessage() for r in caplog.records)
        # At least the first few ids must be present + the total count
        assert "bulk-0" in combined
        assert "50" in combined  # count mentioned somewhere


# ---------------------------------------------------------------------------
# R4 — long warning/error_message truncated to bounded size
# ---------------------------------------------------------------------------


class TestR4TruncateLongFields:
    LIMIT = 500  # expected truncation threshold

    @pytest.mark.asyncio
    async def test_long_error_message_truncated(self, poller, mock_dbx):
        long_err = "x" * 5000
        results = [
            (_make_event("e1"), SyncStatus.FAILED, None, long_err),
        ]

        captured = {}

        def capture(query, *args, **kwargs):
            captured["sql"] = query
            return []

        mock_dbx.execute_sql = MagicMock(side_effect=capture)

        await poller._batch_finalize(results)

        sql = captured["sql"]
        # The raw 5000-char string must NOT appear in full
        assert "x" * 5000 not in sql, "error_message must be truncated"
        # A truncation marker of some sort must be present
        assert "truncated" in sql.lower(), (
            "truncation marker should indicate the message was cut"
        )
        # The xxx prefix must still be there (evidence it's the same value)
        assert "xxxxx" in sql

    @pytest.mark.asyncio
    async def test_long_warning_truncated(self, poller, mock_dbx):
        long_warn = "w" * 5000
        results = [
            (_make_event("w1"), SyncStatus.COMPLETED,
             OperationResult(success=True, rows_affected=1, warning=long_warn), None),
        ]
        captured = {}
        mock_dbx.execute_sql = MagicMock(side_effect=lambda q, *a, **kw: captured.update(sql=q) or [])

        await poller._batch_finalize(results)

        assert "w" * 5000 not in captured["sql"]
        assert "truncated" in captured["sql"].lower()

    @pytest.mark.asyncio
    async def test_short_message_not_modified(self, poller, mock_dbx):
        short = "short error 42"
        results = [
            (_make_event("e1"), SyncStatus.FAILED, None, short),
        ]
        captured = {}
        mock_dbx.execute_sql = MagicMock(
            side_effect=lambda q, *a, **kw: captured.update(sql=q) or []
        )

        await poller._batch_finalize(results)

        assert short in captured["sql"]
        assert "truncated" not in captured["sql"].lower()

    @pytest.mark.asyncio
    async def test_message_exactly_at_limit_preserved(self, poller, mock_dbx):
        """A message of exactly LIMIT chars should not be truncated."""
        at_limit = "z" * self.LIMIT
        results = [
            (_make_event("e1"), SyncStatus.FAILED, None, at_limit),
        ]
        captured = {}
        mock_dbx.execute_sql = MagicMock(
            side_effect=lambda q, *a, **kw: captured.update(sql=q) or []
        )

        await poller._batch_finalize(results)

        assert at_limit in captured["sql"]
        assert "truncated" not in captured["sql"].lower()

    @pytest.mark.asyncio
    async def test_truncation_also_applied_in_legacy_update_event_status(
        self, poller, mock_dbx
    ):
        """The legacy single-row path must truncate too, for consistency."""
        long_err = "y" * 5000
        captured = {}
        mock_dbx.execute_sql = MagicMock(
            side_effect=lambda q, *a, **kw: captured.update(sql=q) or []
        )

        await poller.update_event_status(
            _make_event("legacy"),
            SyncStatus.FAILED,
            error_message=long_err,
        )

        assert "y" * 5000 not in captured["sql"]
        assert "truncated" in captured["sql"].lower()

    @pytest.mark.asyncio
    async def test_none_values_stay_null(self, poller, mock_dbx):
        """None warning/error must produce NULL, not be affected by truncation."""
        results = [
            (_make_event("ok"), SyncStatus.COMPLETED,
             OperationResult(success=True, rows_affected=1), None),
        ]
        captured = {}
        mock_dbx.execute_sql = MagicMock(
            side_effect=lambda q, *a, **kw: captured.update(sql=q) or []
        )

        await poller._batch_finalize(results)

        sql = captured["sql"]
        # Both warning and error_message should be NULL for the success case
        assert "warning" in sql
        assert "error_message" in sql
        # Must not contain our truncation marker spuriously
        assert "truncated" not in sql.lower()


# ---------------------------------------------------------------------------
# S3 — startup validates poller warehouse existence
# ---------------------------------------------------------------------------


class TestS3WarehouseValidation:
    """main.py exposes _validate_poller_warehouse for testability."""

    @pytest.mark.asyncio
    async def test_validate_warehouse_returns_true_for_existing(self):
        from sql_databricks_bridge.main import _validate_poller_warehouse

        fake_client = MagicMock()
        fake_client.client.warehouses.get = MagicMock(return_value=MagicMock(id="wh-1"))

        ok = await _validate_poller_warehouse(fake_client, "wh-1")
        assert ok is True
        fake_client.client.warehouses.get.assert_called_once_with("wh-1")

    @pytest.mark.asyncio
    async def test_validate_warehouse_returns_false_on_exception(self, caplog):
        from sql_databricks_bridge.main import _validate_poller_warehouse

        fake_client = MagicMock()
        fake_client.client.warehouses.get = MagicMock(
            side_effect=Exception("NOT_FOUND")
        )

        with caplog.at_level(logging.WARNING, logger="sql_databricks_bridge.main"):
            ok = await _validate_poller_warehouse(fake_client, "wh-missing")

        assert ok is False
        combined = " ".join(r.getMessage() for r in caplog.records)
        assert "wh-missing" in combined, "log must mention the invalid warehouse id"


# ---------------------------------------------------------------------------
# P1 — concurrent poll_cycle calls are serialized / skipped
# ---------------------------------------------------------------------------


class TestP1ConcurrentPollCycle:
    @pytest.mark.asyncio
    async def test_second_poll_cycle_skipped_when_first_in_progress(
        self, poller, mock_dbx
    ):
        """Two concurrent poll_cycle calls must NOT double-process events."""
        rows = [_databricks_row(f"evt-{i}") for i in range(3)]

        # Each poll_cycle issues: SELECT + mark + finalize.  Infinite supply.
        mock_dbx.execute_sql = MagicMock(side_effect=lambda *a, **kw: rows if "SELECT" in a[0] else [])

        # Slow the operator so the first cycle hangs inside process_event,
        # giving the second cycle a chance to observe the lock.
        processed = []
        process_started = asyncio.Event()
        release_processing = asyncio.Event()

        async def slow_process(event):
            processed.append(event.event_id)
            process_started.set()
            await release_processing.wait()
            return OperationResult(success=True, rows_affected=1)

        operator_mock = MagicMock()
        operator_mock.process_event = AsyncMock(side_effect=slow_process)
        poller._operator = operator_mock

        # Start first cycle; wait until it's inside process_event
        task1 = asyncio.create_task(poller.poll_cycle())
        await asyncio.wait_for(process_started.wait(), timeout=2.0)

        # Start second cycle — must skip and return 0 immediately
        n2 = await asyncio.wait_for(poller.poll_cycle(), timeout=1.0)
        assert n2 == 0, "Second concurrent poll_cycle must skip, not double-process"

        # Release the first cycle
        release_processing.set()
        n1 = await task1
        assert n1 == 3

        # Operator must have processed exactly 3 events total (not 6)
        assert operator_mock.process_event.call_count == 3

    @pytest.mark.asyncio
    async def test_sequential_poll_cycles_both_run(self, poller, mock_dbx):
        """After the first cycle releases the lock, a new cycle must run."""
        rows = [_databricks_row("e1")]
        mock_dbx.execute_sql = MagicMock(
            side_effect=lambda q, *a, **kw: rows if "SELECT" in q else []
        )

        operator_mock = MagicMock()
        operator_mock.process_event = AsyncMock(
            return_value=OperationResult(success=True, rows_affected=1)
        )
        poller._operator = operator_mock

        n1 = await poller.poll_cycle()
        n2 = await poller.poll_cycle()

        assert n1 == 1
        assert n2 == 1
        assert operator_mock.process_event.call_count == 2
