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
        warehouse_id: str | None = None,
    ) -> None:
        """Initialize event poller.

        Args:
            databricks_client: Databricks client.
            sql_client: SQL Server client.
            permission_manager: Permission manager for access control.
            events_table: Databricks table containing events.
            poll_interval: Seconds between poll cycles.
            max_events_per_poll: Maximum events per cycle.
            warehouse_id: Dedicated SQL Warehouse for this poller. When
                provided, every Databricks query issued by the poller uses
                this warehouse instead of the client default — avoiding
                contention with diff-sync/trigger running on the main one.
        """
        self.databricks = databricks_client
        self.sql = sql_client
        self.permission_manager = permission_manager
        self.events_table = events_table
        self.poll_interval = poll_interval
        self.max_events_per_poll = max_events_per_poll
        self._warehouse_id = warehouse_id or None

        self._running = False
        self._operator: SyncOperator | None = None
        self._on_event_processed: Callable[[SyncEvent], None] | None = None

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
        return await asyncio.to_thread(
            self.databricks.execute_sql, query, self._warehouse_id
        )

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
        if warning:
            updates.append(f"warning = '{_esc(warning)}'")
        if error_message:
            updates.append(f"error_message = '{_esc(error_message)}'")

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
            logger.error(f"Failed to batch-mark events as processing: {e}")

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
                val = pick(res, err)
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
            logger.error(f"Failed to batch-finalize events: {e}")

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
        """Execute one poll cycle."""
        events = await self.query_pending_events()
        if not events:
            return 0

        logger.info(f"Found {len(events)} pending events")

        # 1 query: mark all as processing
        await self._batch_mark_processing(events)

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
            f"max events: {self.max_events_per_poll}, "
            f"warehouse: {self._warehouse_id or 'default'})"
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
