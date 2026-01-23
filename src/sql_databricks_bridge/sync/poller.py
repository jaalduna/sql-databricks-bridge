"""Event polling loop for Databricks → SQL sync."""

import asyncio
import logging
from datetime import datetime
from typing import Callable

from sql_databricks_bridge.auth.permissions import PermissionManager
from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.sql_server import SQLServerClient
from sql_databricks_bridge.models.events import SyncEvent, SyncOperation, SyncStatus
from sql_databricks_bridge.sync.operations import SyncOperator
from sql_databricks_bridge.sync.retry import RetryExhaustedError, retry_async

logger = logging.getLogger(__name__)


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
            databricks_client: Databricks client.
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

    @property
    def operator(self) -> SyncOperator:
        """Get or create sync operator."""
        if self._operator is None:
            settings = get_settings()
            self._operator = SyncOperator(
                sql_client=self.sql,
                databricks_client=self.databricks,
                max_delete_rows=10000,  # Default limit
            )
        return self._operator

    def set_event_callback(self, callback: Callable[[SyncEvent], None]) -> None:
        """Set callback to be called after each event is processed."""
        self._on_event_processed = callback

    async def query_pending_events(self) -> list[SyncEvent]:
        """Query pending events from Databricks.

        Returns:
            List of pending sync events, ordered by priority.
        """
        query = f"""
            SELECT *
            FROM {self.events_table}
            WHERE status = 'pending'
            ORDER BY priority DESC, created_at ASC
            LIMIT {self.max_events_per_poll}
        """

        try:
            results = self.databricks.execute_sql(query)

            events = []
            for row in results:
                event = SyncEvent(
                    event_id=row["event_id"],
                    operation=SyncOperation(row["operation"]),
                    source_table=row["source_table"],
                    target_table=row["target_table"],
                    primary_keys=row.get("primary_keys", []) or [],
                    priority=row.get("priority", 0),
                    status=SyncStatus(row.get("status", "pending")),
                    rows_expected=row.get("rows_expected"),
                    created_at=row.get("created_at"),
                    filter_conditions=row.get("filter_conditions", {}) or {},
                    metadata=row.get("metadata", {}) or {},
                )
                events.append(event)

            return events

        except Exception as e:
            logger.error(f"Failed to query pending events: {e}")
            return []

    async def update_event_status(
        self,
        event: SyncEvent,
        status: SyncStatus,
        rows_affected: int | None = None,
        discrepancy: int | None = None,
        warning: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """Update event status in Databricks.

        Args:
            event: Event to update.
            status: New status.
            rows_affected: Number of affected rows.
            discrepancy: Discrepancy between expected and actual.
            warning: Warning message.
            error_message: Error message if failed.
        """
        updates = [f"status = '{status.value}'"]
        updates.append(f"processed_at = current_timestamp()")

        if rows_affected is not None:
            updates.append(f"rows_affected = {rows_affected}")

        if discrepancy is not None:
            updates.append(f"discrepancy = {discrepancy}")

        if warning:
            escaped_warning = warning.replace("'", "''")
            updates.append(f"warning = '{escaped_warning}'")

        if error_message:
            escaped_error = error_message.replace("'", "''")
            updates.append(f"error_message = '{escaped_error}'")

        query = f"""
            UPDATE {self.events_table}
            SET {', '.join(updates)}
            WHERE event_id = '{event.event_id}'
        """

        try:
            self.databricks.execute_sql(query)
        except Exception as e:
            logger.error(f"Failed to update event {event.event_id}: {e}")

    async def process_event(self, event: SyncEvent) -> None:
        """Process a single sync event.

        Args:
            event: Event to process.
        """
        logger.info(
            f"Processing event {event.event_id}: "
            f"{event.operation.value} {event.source_table} → {event.target_table}"
        )

        # Mark as processing
        await self.update_event_status(event, SyncStatus.PROCESSING)

        try:
            # Execute with retry
            result = await retry_async(
                self.operator.process_event,
                event,
                max_attempts=3,
                base_delay=1.0,
                max_delay=30.0,
            )

            if result.success:
                await self.update_event_status(
                    event,
                    SyncStatus.COMPLETED,
                    rows_affected=result.rows_affected,
                    discrepancy=result.discrepancy,
                    warning=result.warning,
                )
                logger.info(
                    f"Event {event.event_id} completed: "
                    f"{result.rows_affected} rows affected"
                )
            else:
                await self.update_event_status(
                    event,
                    SyncStatus.FAILED,
                    error_message=result.error,
                )
                logger.error(f"Event {event.event_id} failed: {result.error}")

        except RetryExhaustedError as e:
            await self.update_event_status(
                event,
                SyncStatus.FAILED,
                error_message=f"Exhausted {e.attempts} retries: {e.last_error}",
            )
            logger.error(f"Event {event.event_id} exhausted retries: {e}")

        except Exception as e:
            await self.update_event_status(
                event,
                SyncStatus.FAILED,
                error_message=str(e),
            )
            logger.exception(f"Event {event.event_id} failed unexpectedly: {e}")

        # Call callback if set
        if self._on_event_processed:
            self._on_event_processed(event)

    async def poll_cycle(self) -> int:
        """Execute one poll cycle.

        Returns:
            Number of events processed.
        """
        events = await self.query_pending_events()

        if not events:
            return 0

        logger.info(f"Found {len(events)} pending events")

        for event in events:
            await self.process_event(event)

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
