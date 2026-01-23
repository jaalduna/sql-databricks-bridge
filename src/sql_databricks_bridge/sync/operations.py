"""Sync operations: INSERT, UPDATE, DELETE."""

import logging
from dataclasses import dataclass
from datetime import datetime

import polars as pl
from sqlalchemy import text

from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.sql_server import SQLServerClient
from sql_databricks_bridge.models.events import SyncEvent, SyncOperation, SyncStatus
from sql_databricks_bridge.sync.validators import (
    ValidationError,
    build_where_clause,
    check_duplicate_primary_keys,
    validate_data_has_primary_keys,
    validate_delete_limit,
    validate_primary_keys,
    validate_source_table,
    validate_table_name,
)

logger = logging.getLogger(__name__)


@dataclass
class OperationResult:
    """Result of a sync operation."""

    success: bool
    rows_affected: int
    discrepancy: int | None = None
    warning: str | None = None
    error: str | None = None
    duration_seconds: float = 0.0


class SyncOperator:
    """Executes sync operations between Databricks and SQL Server."""

    def __init__(
        self,
        sql_client: SQLServerClient,
        databricks_client: DatabricksClient,
        max_delete_rows: int | None = None,
    ) -> None:
        """Initialize sync operator.

        Args:
            sql_client: SQL Server client.
            databricks_client: Databricks client.
            max_delete_rows: Maximum rows allowed for DELETE (None = no limit).
        """
        self.sql = sql_client
        self.databricks = databricks_client
        self.max_delete_rows = max_delete_rows

    async def process_event(self, event: SyncEvent) -> OperationResult:
        """Process a sync event.

        Args:
            event: Event to process.

        Returns:
            Operation result.
        """
        start_time = datetime.utcnow()

        try:
            # Validate event
            validate_primary_keys(event)

            # Route to appropriate operation
            if event.operation == SyncOperation.INSERT:
                result = await self._execute_insert(event)
            elif event.operation == SyncOperation.UPDATE:
                result = await self._execute_update(event)
            elif event.operation == SyncOperation.DELETE:
                result = await self._execute_delete(event)
            else:
                raise ValueError(f"Unknown operation: {event.operation}")

            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            return result

        except ValidationError as e:
            logger.error(f"Validation error for event {event.event_id}: {e}")
            return OperationResult(
                success=False,
                rows_affected=0,
                error=str(e),
                duration_seconds=(datetime.utcnow() - start_time).total_seconds(),
            )

        except Exception as e:
            logger.exception(f"Error processing event {event.event_id}: {e}")
            return OperationResult(
                success=False,
                rows_affected=0,
                error=str(e),
                duration_seconds=(datetime.utcnow() - start_time).total_seconds(),
            )

    async def _execute_insert(self, event: SyncEvent) -> OperationResult:
        """Execute INSERT operation.

        Args:
            event: Sync event.

        Returns:
            Operation result.
        """
        # Parse source table
        catalog, schema, table = validate_source_table(event.source_table)

        # Read data from Databricks
        source_query = f"SELECT * FROM {event.source_table}"
        df = await self._read_from_databricks(source_query)

        if df.is_empty():
            return OperationResult(
                success=True,
                rows_affected=0,
                warning="No data to insert",
            )

        # Check for duplicate PKs if specified
        warning = None
        if event.primary_keys:
            validate_data_has_primary_keys(df, event.primary_keys, event.event_id)
            dup_count = check_duplicate_primary_keys(df, event.primary_keys, event.event_id)
            if dup_count > 0:
                warning = f"Found {dup_count} duplicate primary key rows"

        # Parse target table
        target_schema, target_table = validate_table_name(event.target_table)

        # Bulk insert
        rows_affected = self.sql.bulk_insert(target_table, df, schema=target_schema)

        # Check discrepancy
        expected = event.rows_expected or len(df)
        discrepancy = expected - rows_affected if expected != rows_affected else None

        return OperationResult(
            success=True,
            rows_affected=rows_affected,
            discrepancy=discrepancy,
            warning=warning,
        )

    async def _execute_update(self, event: SyncEvent) -> OperationResult:
        """Execute UPDATE operation.

        Args:
            event: Sync event.

        Returns:
            Operation result.
        """
        # Parse source table
        catalog, schema, table = validate_source_table(event.source_table)

        # Read data from Databricks
        source_query = f"SELECT * FROM {event.source_table}"
        df = await self._read_from_databricks(source_query)

        if df.is_empty():
            return OperationResult(
                success=True,
                rows_affected=0,
                warning="No data to update",
            )

        # Validate PKs
        validate_data_has_primary_keys(df, event.primary_keys, event.event_id)

        # Parse target table
        target_schema, target_table = validate_table_name(event.target_table)

        # Build and execute UPDATE statements
        total_affected = 0
        non_pk_columns = [c for c in df.columns if c not in event.primary_keys]

        for row in df.iter_rows(named=True):
            # Build SET clause
            set_parts = [f"[{col}] = :{col}" for col in non_pk_columns]
            set_clause = ", ".join(set_parts)

            # Build WHERE clause
            where_clause, params = build_where_clause(
                event.primary_keys,
                {pk: row[pk] for pk in event.primary_keys},
            )

            # Add non-PK values to params
            for col in non_pk_columns:
                params[col] = row[col]

            query = f"UPDATE [{target_schema}].[{target_table}] SET {set_clause} WHERE {where_clause}"

            rows = self.sql.execute_write(query, params)
            total_affected += rows

        # Check discrepancy
        expected = event.rows_expected or len(df)
        discrepancy = expected - total_affected if expected != total_affected else None

        return OperationResult(
            success=True,
            rows_affected=total_affected,
            discrepancy=discrepancy,
        )

    async def _execute_delete(self, event: SyncEvent) -> OperationResult:
        """Execute DELETE operation.

        Args:
            event: Sync event.

        Returns:
            Operation result.
        """
        # Parse target table
        target_schema, target_table = validate_table_name(event.target_table)

        # Build WHERE clause
        where_clause, params = build_where_clause(
            event.primary_keys,
            event.filter_conditions,
        )

        # Count rows to delete first
        count_query = (
            f"SELECT COUNT(*) as cnt FROM [{target_schema}].[{target_table}] "
            f"WHERE {where_clause}"
        )

        count_df = self.sql.execute_query(count_query, params)
        rows_to_delete = count_df.item(0, "cnt") if not count_df.is_empty() else 0

        # Validate delete limit
        validate_delete_limit(
            rows_to_delete,
            self.max_delete_rows,
            event.event_id,
        )

        # Execute DELETE
        delete_query = (
            f"DELETE FROM [{target_schema}].[{target_table}] WHERE {where_clause}"
        )

        rows_affected = self.sql.execute_write(delete_query, params)

        # Check discrepancy
        expected = event.rows_expected or rows_to_delete
        discrepancy = expected - rows_affected if expected != rows_affected else None

        return OperationResult(
            success=True,
            rows_affected=rows_affected,
            discrepancy=discrepancy,
        )

    async def _read_from_databricks(self, query: str) -> pl.DataFrame:
        """Read data from Databricks.

        Args:
            query: SQL query to execute.

        Returns:
            DataFrame with query results.
        """
        # Use statement execution API
        results = self.databricks.execute_sql(query)

        if not results:
            return pl.DataFrame()

        return pl.DataFrame(results)
