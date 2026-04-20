"""Event polling loop for Databricks → SQL sync."""

import asyncio
import json
import logging
from typing import Callable

from sql_databricks_bridge.auth.permissions import PermissionManager
from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.sql_server import SQLServerClient
from sql_databricks_bridge.models.events import SyncEvent, SyncOperation, SyncStatus
from sql_databricks_bridge.sync.operations import OperationResult, SyncOperator, TransientConnectionError
from sql_databricks_bridge.sync.retry import RetryExhaustedError, retry_async

logger = logging.getLogger(__name__)


def _esc(v: str) -> str:
    """Escape single quotes for SQL string literals."""
    return v.replace("'", "''")


_MAX_TEXT_FIELD_CHARS = 500
_TRUNCATION_MARKER = "...[truncated]"


def _truncate(value: str | None, limit: int = _MAX_TEXT_FIELD_CHARS) -> str | None:
    """Cap free-text fields (warning/error_message) to keep batched UPDATEs sane.

    A message that is exactly ``limit`` long is preserved as-is; anything
    longer is cut to ``limit - len(marker)`` chars plus the marker, so the
    total still fits in ``limit`` chars.
    """
    if value is None or len(value) <= limit:
        return value
    head = value[: limit - len(_TRUNCATION_MARKER)]
    return head + _TRUNCATION_MARKER


def _format_id_list(event_ids: list[str], limit: int = 10) -> str:
    """Render event ids for error logs — truncates after *limit* ids."""
    if not event_ids:
        return ""
    shown = event_ids[:limit]
    rendered = ",".join(shown)
    if len(event_ids) > limit:
        rendered += f",...(+{len(event_ids) - limit})"
    return rendered


class EventPoller:
    """Polls Databricks for sync events and processes them."""

    def __init__(
        self,
        databricks_client: DatabricksClient,
        sql_client: SQLServerClient,
        permission_manager: PermissionManager | None = None,
        events_table: str = "bridge.events.bridge_events",
        poll_interval: int = 10,
        max_events_per_poll: int = 100,
    ) -> None:
        """Initialize event poller.

        Args:
            databricks_client: Databricks client.  To run the poller on a
                dedicated SQL Warehouse (to avoid contention with diff-sync
                or triggers), pass a client configured with the desired
                warehouse — no extra parameter needed here.
            sql_client: SQL Server client.
            permission_manager: Permission manager for access control.
            events_table: Databricks table containing events.
            poll_interval: Seconds between poll cycles.
            max_events_per_poll: Maximum events per cycle.
        """
        self.databricks = databricks_client
        self.sql = sql_client
        self.permission_manager = permission_manager
        self.events_table = events_table
        self.poll_interval = poll_interval
        self.max_events_per_poll = max_events_per_poll

        self._running = False
        self._operator: SyncOperator | None = None
        self._on_event_processed: Callable[[SyncEvent], None] | None = None
        # P1: prevent two poll_cycle invocations from running simultaneously
        # (scheduler + on-demand API trigger).  Checked non-blockingly so a
        # concurrent call skips instead of queueing up.
        self._cycle_lock = asyncio.Lock()

    @property
    def operator(self) -> SyncOperator:
        """Get or create sync operator."""
        if self._operator is None:
            self._operator = SyncOperator(
                sql_client=self.sql,
                databricks_client=self.databricks,
                max_delete_rows=10000,
            )
        return self._operator

    def set_event_callback(self, callback: Callable[[SyncEvent], None]) -> None:
        """Set callback to be called after each event is processed."""
        self._on_event_processed = callback

    # ------------------------------------------------------------------
    # Databricks I/O — wrapped in asyncio.to_thread so the event loop
    # stays responsive while the Statement Execution API call blocks.
    # ------------------------------------------------------------------

    async def _dbx_exec(self, query: str) -> list[dict]:
        return await asyncio.to_thread(self.databricks.execute_sql, query)

    async def query_pending_events(self) -> list[SyncEvent]:
        """Query pending events from Databricks."""
        query = (
            f"SELECT * FROM {self.events_table} "
            f"WHERE status = 'pending' "
            f"ORDER BY priority DESC, created_at ASC "
            f"LIMIT {self.max_events_per_poll}"
        )

        try:
            results = await self._dbx_exec(query)
        except Exception as e:
            logger.error(f"Failed to query pending events: {e}")
            return []

        events: list[SyncEvent] = []
        for row in results:
            raw_pks = row.get("primary_keys") or []
            if isinstance(raw_pks, str):
                try:
                    raw_pks = json.loads(raw_pks)
                except (json.JSONDecodeError, TypeError):
                    raw_pks = []

            raw_filters = row.get("filter_conditions") or {}
            if isinstance(raw_filters, str):
                try:
                    raw_filters = json.loads(raw_filters)
                except (json.JSONDecodeError, TypeError):
                    raw_filters = {}

            raw_meta = row.get("metadata") or {}
            if isinstance(raw_meta, str):
                try:
                    raw_meta = json.loads(raw_meta)
                except (json.JSONDecodeError, TypeError):
                    raw_meta = {}

            events.append(SyncEvent(
                event_id=row["event_id"],
                operation=SyncOperation(row["operation"]),
                source_table=row["source_table"],
                target_table=row["target_table"],
                primary_keys=raw_pks,
                priority=row.get("priority") or 0,
                status=SyncStatus(row.get("status", "pending")),
                rows_expected=row.get("rows_expected"),
                created_at=row.get("created_at"),
                filter_conditions=raw_filters,
                metadata=raw_meta,
            ))
        return events

    async def update_event_status(
        self,
        event: SyncEvent,
        status: SyncStatus,
        rows_affected: int | None = None,
        discrepancy: int | None = None,
        warning: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """Update a single event's status (legacy/single-row entrypoint).

        Kept for backwards-compatibility with callers that drive events one
        at a time.  The polling loop itself uses the batched variants below.
        """
        updates = [f"status = '{status.value}'", "processed_at = current_timestamp()"]
        if rows_affected is not None:
            updates.append(f"rows_affected = {rows_affected}")
        if discrepancy is not None:
            updates.append(f"discrepancy = {discrepancy}")
        # R4 also applies here: bound free-text fields on the legacy path.
        warning_t = _truncate(warning)
        error_t = _truncate(error_message)
        if warning_t:
            updates.append(f"warning = '{_esc(warning_t)}'")
        if error_t:
            updates.append(f"error_message = '{_esc(error_t)}'")

        query = (
            f"UPDATE {self.events_table} "
            f"SET {', '.join(updates)} "
            f"WHERE event_id = '{_esc(event.event_id)}'"
        )
        try:
            await self._dbx_exec(query)
        except Exception as e:
            logger.error(f"Failed to update event {event.event_id}: {e}")

    # ------------------------------------------------------------------
    # Batched status updates — a single UPDATE for the batch, instead of
    # one per event.  For the final step we use CASE WHEN per column.
    # ------------------------------------------------------------------

    async def _batch_mark_processing(self, events: list[SyncEvent]) -> None:
        """Mark a batch of events as 'processing'.

        Re-raises on failure so callers (poll_cycle) can abort the cycle
        cleanly instead of proceeding with work that may not be recoverable.
        """
        if not events:
            return
        id_list = ",".join(f"'{_esc(e.event_id)}'" for e in events)
        query = (
            f"UPDATE {self.events_table} "
            f"SET status = 'processing', processed_at = current_timestamp() "
            f"WHERE event_id IN ({id_list})"
        )
        try:
            await self._dbx_exec(query)
        except Exception as e:
            ids = _format_id_list([ev.event_id for ev in events])
            logger.error(
                "Failed to batch-mark %d events as processing: %s | ids=[%s]",
                len(events), e, ids,
            )
            raise

    async def _batch_finalize(
        self,
        results: list[tuple[SyncEvent, SyncStatus, OperationResult | None, str | None]],
    ) -> None:
        """Finalize batch results in a single UPDATE with CASE WHEN per column.

        Each tuple is (event, final_status, op_result_or_None, error_message_or_None).
        op_result is None when the event failed outside of operator.process_event
        (e.g. RetryExhaustedError / unexpected exception).
        """
        if not results:
            return

        def case_status() -> str:
            parts = [
                f"WHEN '{_esc(e.event_id)}' THEN '{status.value}'"
                for e, status, _, _ in results
            ]
            return "CASE event_id " + " ".join(parts) + " END"

        def case_numeric(pick) -> str:
            parts = []
            for e, _, res, _ in results:
                val = pick(res)
                val_sql = "NULL" if val is None else str(int(val))
                parts.append(f"WHEN '{_esc(e.event_id)}' THEN {val_sql}")
            return "CASE event_id " + " ".join(parts) + " END"

        def case_str(pick) -> str:
            parts = []
            for e, _, res, err in results:
                val = _truncate(pick(res, err))
                val_sql = "NULL" if val is None else f"'{_esc(str(val))}'"
                parts.append(f"WHEN '{_esc(e.event_id)}' THEN {val_sql}")
            return "CASE event_id " + " ".join(parts) + " END"

        id_list = ",".join(f"'{_esc(e.event_id)}'" for e, _, _, _ in results)

        set_clauses = [
            f"status = {case_status()}",
            "processed_at = current_timestamp()",
            f"rows_affected = {case_numeric(lambda r: r.rows_affected if r else None)}",
            f"discrepancy = {case_numeric(lambda r: r.discrepancy if r else None)}",
            f"warning = {case_str(lambda r, err: (r.warning if r else None))}",
            f"error_message = {case_str(lambda r, err: (err if err else (r.error if r else None)))}",
        ]

        query = (
            f"UPDATE {self.events_table} "
            f"SET {', '.join(set_clauses)} "
            f"WHERE event_id IN ({id_list})"
        )
        try:
            await self._dbx_exec(query)
        except Exception as e:
            ids = _format_id_list([ev.event_id for ev, *_ in results])
            logger.error(
                "Failed to batch-finalize %d events: %s | ids=[%s]",
                len(results), e, ids,
            )

    # ------------------------------------------------------------------
    # Event processing (still serial to avoid overwhelming SQL Server)
    # ------------------------------------------------------------------

    async def _process_event_collect(
        self, event: SyncEvent
    ) -> tuple[SyncEvent, SyncStatus, OperationResult | None, str | None]:
        """Process a single event and return (event, status, op_result, error).

        Does NOT write status to Databricks — the batch finalize call does that
        for the whole cycle.
        """
        logger.info(
            f"Processing event {event.event_id}: "
            f"{event.operation.value} {event.source_table} → {event.target_table}"
        )

        try:
            result = await retry_async(
                self.operator.process_event,
                event,
                max_attempts=3,
                base_delay=2.0,
                max_delay=60.0,
                retryable_exceptions=(TransientConnectionError,),
            )

            if result.success:
                logger.info(
                    f"Event {event.event_id} completed: "
                    f"{result.rows_affected} rows affected"
                )
                return event, SyncStatus.COMPLETED, result, None
            else:
                logger.error(f"Event {event.event_id} failed: {result.error}")
                return event, SyncStatus.FAILED, result, None

        except RetryExhaustedError as e:
            err = f"Exhausted {e.attempts} retries: {e.last_error}"
            logger.error(f"Event {event.event_id} exhausted retries: {e}")
            return event, SyncStatus.FAILED, None, err
        except Exception as e:
            logger.exception(f"Event {event.event_id} failed unexpectedly: {e}")
            return event, SyncStatus.FAILED, None, str(e)

    # Backwards-compatible single-event entrypoint (used by tests/callers
    # that drive one event at a time).  Runs the same collect logic and
    # then issues a one-row batch update.
    async def process_event(self, event: SyncEvent) -> None:
        await self._batch_mark_processing([event])
        result = await self._process_event_collect(event)
        await self._batch_finalize([result])
        if self._on_event_processed:
            self._on_event_processed(event)

    async def poll_cycle(self) -> int:
        """Execute one poll cycle.

        Serialised against itself: if another cycle is already running, we
        skip this one and return 0 (the next tick will pick up new events).
        """
        # P1: skip — don't queue up behind an in-flight cycle.
        if self._cycle_lock.locked():
            logger.info("poll_cycle skipped: another cycle is already in progress")
            return 0

        async with self._cycle_lock:
            events = await self.query_pending_events()
            if not events:
                return 0

            logger.info(f"Found {len(events)} pending events")

            # R2: if marking fails, abort cleanly — leave events in 'pending'
            # so the next cycle retries, instead of risking orphaned 'processing'
            # rows or double-processing on a later manual reset.
            try:
                await self._batch_mark_processing(events)
            except Exception:
                # The _batch_mark_processing logger already captured ids+error.
                return 0

            # Serial processing (stays as-is to avoid overwhelming SQL Server)
            results: list[tuple[SyncEvent, SyncStatus, OperationResult | None, str | None]] = []
            for event in events:
                results.append(await self._process_event_collect(event))

            # 1 query: finalize all statuses with CASE WHEN
            await self._batch_finalize(results)

            # Fire callbacks (kept as-is)
            if self._on_event_processed:
                for event in events:
                    self._on_event_processed(event)

            return len(events)

    async def start(self) -> None:
        """Start the polling loop."""
        self._running = True
        logger.info(
            f"Starting event poller (interval: {self.poll_interval}s, "
            f"max events: {self.max_events_per_poll})"
        )

        while self._running:
            try:
                events_processed = await self.poll_cycle()
                if events_processed > 0:
                    logger.debug(f"Processed {events_processed} events")
            except Exception as e:
                logger.exception(f"Error in poll cycle: {e}")

            await asyncio.sleep(self.poll_interval)

    def stop(self) -> None:
        """Stop the polling loop."""
        logger.info("Stopping event poller")
        self._running = False

    @property
    def is_running(self) -> bool:
        """Check if poller is running."""
        return self._running
