"""Validators for sync operations."""

import logging
from typing import Any

import polars as pl

from sql_databricks_bridge.models.events import SyncEvent, SyncOperation

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Raised when validation fails."""

    def __init__(self, message: str, event_id: str | None = None):
        super().__init__(message)
        self.event_id = event_id


def validate_primary_keys(event: SyncEvent) -> None:
    """Validate that primary keys are provided for UPDATE/DELETE.

    Args:
        event: Sync event to validate.

    Raises:
        ValidationError: If primary keys are missing.
    """
    if event.operation in (SyncOperation.UPDATE, SyncOperation.DELETE):
        if not event.primary_keys:
            raise ValidationError(
                f"Primary keys required for {event.operation.value} operation",
                event_id=event.event_id,
            )


def validate_data_has_primary_keys(
    df: pl.DataFrame,
    primary_keys: list[str],
    event_id: str,
) -> None:
    """Validate that DataFrame contains all primary key columns.

    Args:
        df: DataFrame to validate.
        primary_keys: Required primary key columns.
        event_id: Event ID for error reporting.

    Raises:
        ValidationError: If primary key columns are missing.
    """
    missing = [pk for pk in primary_keys if pk not in df.columns]
    if missing:
        raise ValidationError(
            f"Missing primary key columns: {missing}",
            event_id=event_id,
        )


def check_duplicate_primary_keys(
    df: pl.DataFrame,
    primary_keys: list[str],
    event_id: str,
) -> int:
    """Check for duplicate primary keys in data.

    Args:
        df: DataFrame to check.
        primary_keys: Primary key columns.
        event_id: Event ID for logging.

    Returns:
        Number of duplicate rows found.
    """
    if not primary_keys or df.is_empty():
        return 0

    # Count duplicates
    duplicate_count = (
        df.group_by(primary_keys)
        .agg(pl.len().alias("_count"))
        .filter(pl.col("_count") > 1)
        .select(pl.col("_count").sum() - pl.len())
        .item()
    )

    if duplicate_count > 0:
        logger.warning(
            f"Event {event_id}: Found {duplicate_count} duplicate primary key combinations"
        )

    return duplicate_count


def validate_delete_limit(
    rows_to_delete: int,
    max_delete_rows: int | None,
    event_id: str,
) -> None:
    """Validate DELETE operation is within allowed limit.

    Args:
        rows_to_delete: Number of rows to delete.
        max_delete_rows: Maximum allowed (None = no limit).
        event_id: Event ID for error reporting.

    Raises:
        ValidationError: If limit exceeded.
    """
    if max_delete_rows is not None and rows_to_delete > max_delete_rows:
        raise ValidationError(
            f"DELETE limit exceeded: {rows_to_delete} > {max_delete_rows}",
            event_id=event_id,
        )


def validate_table_name(table_name: str) -> tuple[str, str]:
    """Validate and parse table name.

    Args:
        table_name: Table name in format "schema.table".

    Returns:
        Tuple of (schema, table).

    Raises:
        ValidationError: If format is invalid.
    """
    parts = table_name.split(".")

    if len(parts) == 1:
        return "dbo", parts[0]

    if len(parts) == 2:
        return parts[0], parts[1]

    raise ValidationError(f"Invalid table name format: {table_name}")


def validate_source_table(source_table: str) -> tuple[str, str, str]:
    """Validate and parse Databricks source table name.

    Args:
        source_table: Table name in format "catalog.schema.table".

    Returns:
        Tuple of (catalog, schema, table).

    Raises:
        ValidationError: If format is invalid.
    """
    parts = source_table.split(".")

    if len(parts) != 3:
        raise ValidationError(
            f"Invalid Databricks table format: {source_table}. "
            "Expected: catalog.schema.table"
        )

    return parts[0], parts[1], parts[2]


def build_where_clause(
    primary_keys: list[str],
    conditions: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    """Build WHERE clause from primary keys and conditions.

    Args:
        primary_keys: Primary key columns.
        conditions: Column-value conditions.

    Returns:
        Tuple of (WHERE clause, parameters dict).
    """
    clauses = []
    params = {}

    for pk in primary_keys:
        if pk in conditions:
            clauses.append(f"[{pk}] = :pk_{pk}")
            params[f"pk_{pk}"] = conditions[pk]

    for key, value in conditions.items():
        if key not in primary_keys:
            clauses.append(f"[{key}] = :cond_{key}")
            params[f"cond_{key}"] = value

    where_clause = " AND ".join(clauses) if clauses else "1=1"

    return where_clause, params
