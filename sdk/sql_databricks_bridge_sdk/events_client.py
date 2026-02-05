"""Events table client for direct Databricks interaction.

Use this client when working within Databricks jobs or notebooks to interact
directly with the bridge_events table without going through the REST API.
"""

import json
import time
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sql_databricks_bridge_sdk.exceptions import (
    BridgeSDKError,
    ConnectionError,
    EventNotFoundError,
    TimeoutError,
    ValidationError,
)
from sql_databricks_bridge_sdk.models import (
    EventResult,
    SyncEvent,
    SyncEventStatus,
    SyncOperation,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class BridgeEventsClient:
    """Client for interacting directly with the bridge_events table in Databricks.

    Use this client within Databricks jobs or notebooks to create and monitor
    sync events without going through the REST API.

    Example:
        ```python
        from sql_databricks_bridge_sdk import BridgeEventsClient

        client = BridgeEventsClient()

        # Create INSERT event
        event_id = client.create_insert_event(
            source_table="my-catalog.my-schema.source_data",
            target_table="dbo.MarketScopeAccess",
            primary_keys=["username"],
            rows_expected=100
        )

        # Wait for completion
        result = client.wait_for_completion(event_id)
        print(f"Synced {result.rows_affected} rows")
        ```
    """

    DEFAULT_CATALOG = "000-sql-databricks-bridge"
    DEFAULT_SCHEMA = "events"
    DEFAULT_TABLE = "bridge_events"

    def __init__(
        self,
        catalog: str | None = None,
        schema: str | None = None,
        table: str | None = None,
        spark: "SparkSession | None" = None,
    ):
        """Initialize the events client.

        Args:
            catalog: Databricks catalog name (default: 000-sql-databricks-bridge)
            schema: Schema name (default: events)
            table: Events table name (default: bridge_events)
            spark: SparkSession (auto-detected if not provided)
        """
        self.catalog = catalog or self.DEFAULT_CATALOG
        self.schema = schema or self.DEFAULT_SCHEMA
        self.table = table or self.DEFAULT_TABLE
        self._spark = spark

    @property
    def spark(self) -> "SparkSession":
        """Get or create SparkSession."""
        if self._spark is None:
            try:
                from pyspark.sql import SparkSession

                self._spark = SparkSession.builder.getOrCreate()
            except ImportError as e:
                raise ConnectionError(
                    "PySpark not available. This client requires a Databricks environment."
                ) from e
        return self._spark

    @property
    def events_table(self) -> str:
        """Get fully qualified events table name."""
        return f"`{self.catalog}`.`{self.schema}`.`{self.table}`"

    def _execute_sql(self, sql: str) -> list[dict[str, Any]]:
        """Execute SQL and return results as list of dicts.

        Args:
            sql: SQL statement to execute

        Returns:
            List of row dictionaries
        """
        try:
            df = self.spark.sql(sql)
            rows = df.collect()
            columns = df.columns
            return [{col: row[col] for col in columns} for row in rows]
        except Exception as e:
            raise BridgeSDKError(f"SQL execution failed: {e}") from e

    def _generate_event_id(self, prefix: str) -> str:
        """Generate a unique event ID."""
        return f"{prefix}-{uuid.uuid4().hex[:12]}"

    def create_event(
        self,
        operation: SyncOperation | str,
        source_table: str,
        target_table: str,
        primary_keys: list[str] | None = None,
        rows_expected: int | None = None,
        filter_conditions: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        priority: int = 5,
        event_id: str | None = None,
    ) -> str:
        """Create a sync event in the bridge_events table.

        Args:
            operation: Type of operation (INSERT, UPDATE, DELETE)
            source_table: Source table in Databricks (catalog.schema.table)
            target_table: Target table in SQL Server (schema.table)
            primary_keys: Primary key columns for UPDATE/DELETE
            rows_expected: Expected number of rows (for validation)
            filter_conditions: WHERE conditions for DELETE
            metadata: Additional metadata
            priority: Event priority (1-10, lower = higher priority)
            event_id: Custom event ID (auto-generated if not provided)

        Returns:
            Event ID

        Raises:
            ValidationError: If required parameters are missing
        """
        if isinstance(operation, str):
            operation = SyncOperation(operation)

        # Validate
        if operation in (SyncOperation.UPDATE, SyncOperation.DELETE):
            if not primary_keys:
                raise ValidationError(
                    f"primary_keys required for {operation.value} operation"
                )

        if operation == SyncOperation.DELETE and not filter_conditions:
            raise ValidationError("filter_conditions required for DELETE operation")

        # Generate event ID
        if not event_id:
            event_id = self._generate_event_id(operation.value.lower())

        # Build primary keys array
        pk_array = ", ".join(f"'{pk}'" for pk in (primary_keys or []))

        # Build filter conditions JSON
        filter_json = ""
        if filter_conditions:
            filter_json = json.dumps(filter_conditions).replace("'", "\\'")

        # Build metadata JSON
        metadata_json = ""
        if metadata:
            metadata_json = json.dumps(metadata).replace("'", "\\'")

        # Build INSERT statement
        sql = f"""
            INSERT INTO {self.events_table}
            (event_id, operation, source_table, target_table, primary_keys,
             priority, status, rows_expected, filter_conditions, metadata, created_at)
            VALUES (
                '{event_id}',
                '{operation.value}',
                '{source_table}',
                '{target_table}',
                ARRAY({pk_array}),
                {priority},
                'pending',
                {rows_expected if rows_expected is not None else 'NULL'},
                {f"'{filter_json}'" if filter_json else 'NULL'},
                {f"'{metadata_json}'" if metadata_json else 'NULL'},
                current_timestamp()
            )
        """

        self._execute_sql(sql)
        return event_id

    def create_insert_event(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str] | None = None,
        rows_expected: int | None = None,
        metadata: dict[str, Any] | None = None,
        priority: int = 5,
    ) -> str:
        """Create an INSERT sync event.

        Args:
            source_table: Source table in Databricks (catalog.schema.table)
            target_table: Target table in SQL Server (schema.table)
            primary_keys: Primary key columns (for duplicate detection)
            rows_expected: Expected number of rows
            metadata: Additional metadata
            priority: Event priority (1-10)

        Returns:
            Event ID
        """
        return self.create_event(
            operation=SyncOperation.INSERT,
            source_table=source_table,
            target_table=target_table,
            primary_keys=primary_keys,
            rows_expected=rows_expected,
            metadata=metadata,
            priority=priority,
        )

    def create_update_event(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        rows_expected: int | None = None,
        metadata: dict[str, Any] | None = None,
        priority: int = 5,
    ) -> str:
        """Create an UPDATE sync event.

        Args:
            source_table: Source table with updated data
            target_table: Target table in SQL Server
            primary_keys: Primary key columns (required)
            rows_expected: Expected number of rows
            metadata: Additional metadata
            priority: Event priority (1-10)

        Returns:
            Event ID
        """
        return self.create_event(
            operation=SyncOperation.UPDATE,
            source_table=source_table,
            target_table=target_table,
            primary_keys=primary_keys,
            rows_expected=rows_expected,
            metadata=metadata,
            priority=priority,
        )

    def create_delete_event(
        self,
        target_table: str,
        primary_keys: list[str],
        filter_conditions: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        priority: int = 5,
    ) -> str:
        """Create a DELETE sync event.

        Args:
            target_table: Target table in SQL Server
            primary_keys: Primary key columns (required)
            filter_conditions: WHERE conditions (required)
            metadata: Additional metadata
            priority: Event priority (1-10)

        Returns:
            Event ID
        """
        return self.create_event(
            operation=SyncOperation.DELETE,
            source_table="",
            target_table=target_table,
            primary_keys=primary_keys,
            filter_conditions=filter_conditions,
            metadata=metadata,
            priority=priority,
        )

    def get_event(self, event_id: str) -> SyncEvent:
        """Get an event by ID.

        Args:
            event_id: Event ID

        Returns:
            SyncEvent

        Raises:
            EventNotFoundError: If event not found
        """
        sql = f"""
            SELECT
                event_id, operation, source_table, target_table,
                primary_keys, priority, status, rows_expected, rows_affected,
                discrepancy, warning, error_message, filter_conditions,
                metadata, created_at, processed_at
            FROM {self.events_table}
            WHERE event_id = '{event_id}'
        """

        results = self._execute_sql(sql)

        if not results:
            raise EventNotFoundError(event_id)

        row = results[0]

        # Parse primary_keys from Spark array
        primary_keys = row.get("primary_keys", [])
        if primary_keys is None:
            primary_keys = []

        return SyncEvent(
            event_id=row["event_id"],
            operation=SyncOperation(row["operation"]),
            source_table=row.get("source_table", ""),
            target_table=row.get("target_table", ""),
            primary_keys=list(primary_keys),
            priority=row.get("priority", 5),
            status=SyncEventStatus(row["status"]),
            rows_expected=row.get("rows_expected"),
            rows_affected=row.get("rows_affected"),
            discrepancy=row.get("discrepancy"),
            warning=row.get("warning"),
            error_message=row.get("error_message"),
            filter_conditions=json.loads(row["filter_conditions"]) if row.get("filter_conditions") else None,
            metadata=json.loads(row["metadata"]) if row.get("metadata") else None,
            created_at=row.get("created_at"),
            processed_at=row.get("processed_at"),
        )

    def get_status(self, event_id: str) -> SyncEvent:
        """Get event status (alias for get_event).

        Args:
            event_id: Event ID

        Returns:
            SyncEvent with current status
        """
        return self.get_event(event_id)

    def list_events(
        self,
        status: SyncEventStatus | str | None = None,
        operation: SyncOperation | str | None = None,
        limit: int = 100,
    ) -> list[SyncEvent]:
        """List sync events.

        Args:
            status: Filter by status
            operation: Filter by operation type
            limit: Maximum number of events to return

        Returns:
            List of SyncEvent objects
        """
        conditions = []

        if status:
            status_val = status.value if isinstance(status, SyncEventStatus) else status
            conditions.append(f"status = '{status_val}'")

        if operation:
            op_val = operation.value if isinstance(operation, SyncOperation) else operation
            conditions.append(f"operation = '{op_val}'")

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        sql = f"""
            SELECT
                event_id, operation, source_table, target_table,
                primary_keys, priority, status, rows_expected, rows_affected,
                discrepancy, warning, error_message, created_at, processed_at
            FROM {self.events_table}
            {where_clause}
            ORDER BY created_at DESC
            LIMIT {limit}
        """

        results = self._execute_sql(sql)

        events = []
        for row in results:
            primary_keys = row.get("primary_keys", [])
            if primary_keys is None:
                primary_keys = []

            events.append(
                SyncEvent(
                    event_id=row["event_id"],
                    operation=SyncOperation(row["operation"]),
                    source_table=row.get("source_table", ""),
                    target_table=row.get("target_table", ""),
                    primary_keys=list(primary_keys),
                    priority=row.get("priority", 5),
                    status=SyncEventStatus(row["status"]),
                    rows_expected=row.get("rows_expected"),
                    rows_affected=row.get("rows_affected"),
                    discrepancy=row.get("discrepancy"),
                    warning=row.get("warning"),
                    error_message=row.get("error_message"),
                    created_at=row.get("created_at"),
                    processed_at=row.get("processed_at"),
                )
            )

        return events

    def list_pending_events(self, limit: int = 100) -> list[SyncEvent]:
        """List pending events.

        Args:
            limit: Maximum number of events

        Returns:
            List of pending SyncEvent objects
        """
        return self.list_events(status=SyncEventStatus.PENDING, limit=limit)

    def wait_for_completion(
        self,
        event_id: str,
        timeout_seconds: int = 300,
        poll_interval: int = 10,
        verbose: bool = False,
    ) -> EventResult:
        """Wait for an event to complete.

        Args:
            event_id: Event ID to wait for
            timeout_seconds: Maximum time to wait
            poll_interval: Seconds between status checks
            verbose: Print status updates

        Returns:
            EventResult with final status

        Raises:
            TimeoutError: If event doesn't complete in time
            EventNotFoundError: If event not found
        """
        start_time = time.time()

        while True:
            elapsed = time.time() - start_time

            if elapsed >= timeout_seconds:
                raise TimeoutError(event_id, timeout_seconds)

            event = self.get_event(event_id)

            if verbose:
                print(f"[{int(elapsed)}s] Event {event_id}: {event.status.value}")

            if event.is_completed:
                return EventResult.from_event(event, duration_seconds=elapsed)

            time.sleep(poll_interval)

    def get_stats(self) -> dict[str, Any]:
        """Get statistics about events.

        Returns:
            Dictionary with total, by_status, by_operation counts
        """
        # Total count
        total_sql = f"SELECT COUNT(*) as total FROM {self.events_table}"
        total_result = self._execute_sql(total_sql)
        total = total_result[0]["total"] if total_result else 0

        # By status
        status_sql = f"""
            SELECT status, COUNT(*) as count
            FROM {self.events_table}
            GROUP BY status
        """
        status_results = self._execute_sql(status_sql)
        by_status = {row["status"]: row["count"] for row in status_results}

        # By operation
        operation_sql = f"""
            SELECT operation, COUNT(*) as count
            FROM {self.events_table}
            GROUP BY operation
        """
        operation_results = self._execute_sql(operation_sql)
        by_operation = {row["operation"]: row["count"] for row in operation_results}

        return {
            "total": total,
            "by_status": by_status,
            "by_operation": by_operation,
        }

    def delete_event(self, event_id: str) -> bool:
        """Delete an event from the table.

        Args:
            event_id: Event ID to delete

        Returns:
            True if deleted
        """
        sql = f"DELETE FROM {self.events_table} WHERE event_id = '{event_id}'"
        self._execute_sql(sql)
        return True

    def cleanup_completed_events(self, days_old: int = 7) -> int:
        """Delete old completed events.

        Args:
            days_old: Delete events older than this many days

        Returns:
            Number of deleted events
        """
        # Count first
        count_sql = f"""
            SELECT COUNT(*) as count
            FROM {self.events_table}
            WHERE status IN ('completed', 'failed')
              AND created_at < date_sub(current_timestamp(), {days_old})
        """
        count_result = self._execute_sql(count_sql)
        count = count_result[0]["count"] if count_result else 0

        if count > 0:
            delete_sql = f"""
                DELETE FROM {self.events_table}
                WHERE status IN ('completed', 'failed')
                  AND created_at < date_sub(current_timestamp(), {days_old})
            """
            self._execute_sql(delete_sql)

        return count
