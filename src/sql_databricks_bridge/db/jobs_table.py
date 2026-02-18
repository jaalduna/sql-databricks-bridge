"""Databricks Delta table CRUD for trigger job history."""

import json
import logging
from datetime import datetime
from typing import Any

from sql_databricks_bridge.db.databricks import DatabricksClient

logger = logging.getLogger(__name__)


def _esc(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    return value.replace("'", "''")


def ensure_jobs_table(client: DatabricksClient, table: str) -> None:
    """Create the trigger_jobs table if it does not exist."""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            job_id STRING NOT NULL,
            country STRING NOT NULL,
            stage STRING NOT NULL,
            tag STRING NOT NULL,
            status STRING NOT NULL,
            queries STRING,
            triggered_by STRING NOT NULL,
            created_at TIMESTAMP,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            error STRING,
            failed_queries STRING,
            period STRING,
            aggregations STRING
        ) USING DELTA
    """
    client.execute_sql(ddl)

    # Migrate existing tables: add columns that may not exist yet
    for col in ("period STRING", "aggregations STRING"):
        try:
            client.execute_sql(f"ALTER TABLE {table} ADD COLUMN {col}")
            logger.info(f"Added column {col.split()[0]} to {table}")
        except Exception:
            pass  # Column already exists
    logger.info(f"Ensured jobs table exists: {table}")


def insert_job(
    client: DatabricksClient,
    table: str,
    *,
    job_id: str,
    country: str,
    stage: str,
    tag: str,
    queries: list[str],
    triggered_by: str,
    created_at: datetime,
    failed_queries: list[str] | None = None,
    period: str | None = None,
    aggregations: dict | None = None,
) -> None:
    """Insert a new job record into the Delta table."""
    queries_json = _esc(json.dumps(queries))
    failed_queries_json = _esc(json.dumps(failed_queries or []))
    period_sql = f"'{_esc(period)}'" if period else "NULL"
    aggregations_sql = f"'{_esc(json.dumps(aggregations))}'" if aggregations else "NULL"
    sql = f"""
        INSERT INTO {table} (job_id, country, stage, tag, status, queries, triggered_by, created_at, failed_queries, period, aggregations)
        VALUES (
            '{_esc(job_id)}',
            '{_esc(country)}',
            '{_esc(stage)}',
            '{_esc(tag)}',
            'pending',
            '{queries_json}',
            '{_esc(triggered_by)}',
            '{created_at.isoformat()}',
            '{failed_queries_json}',
            {period_sql},
            {aggregations_sql}
        )
    """
    client.execute_sql(sql)


def get_job(client: DatabricksClient, table: str, job_id: str) -> dict[str, Any] | None:
    """Get a single job by ID. Returns None if not found."""
    sql = f"SELECT * FROM {table} WHERE job_id = '{_esc(job_id)}'"
    rows = client.execute_sql(sql)
    if not rows:
        return None
    row = rows[0]
    if row.get("queries"):
        row["queries"] = json.loads(row["queries"])
    if row.get("failed_queries"):
        row["failed_queries"] = json.loads(row["failed_queries"])
    else:
        row["failed_queries"] = []
    return row


def list_jobs(
    client: DatabricksClient,
    table: str,
    *,
    country: str | None = None,
    status: str | None = None,
    stage: str | None = None,
    triggered_by: str | None = None,
    period: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    """List jobs with optional filters. Returns (items, total_count)."""
    where_parts: list[str] = []
    if country:
        where_parts.append(f"country = '{_esc(country)}'")
    if status:
        where_parts.append(f"status = '{_esc(status)}'")
    if stage:
        where_parts.append(f"stage = '{_esc(stage)}'")
    if triggered_by:
        where_parts.append(f"triggered_by = '{_esc(triggered_by)}'")
    if period:
        where_parts.append(f"period = '{_esc(period)}'")

    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

    # Single query with COUNT(*) OVER() to get total alongside rows (halves Databricks round-trips)
    select_sql = (
        f"SELECT *, COUNT(*) OVER() AS _total_count FROM {table}{where_sql} "
        f"ORDER BY created_at DESC LIMIT {limit} OFFSET {offset}"
    )
    logger.debug("list_jobs query: %s", select_sql)
    rows = client.execute_sql(select_sql)

    total = 0
    items = []
    for row in rows:
        if total == 0 and "_total_count" in row:
            total = int(row["_total_count"])
        row.pop("_total_count", None)
        if row.get("queries"):
            row["queries"] = json.loads(row["queries"])
        if row.get("failed_queries"):
            row["failed_queries"] = json.loads(row["failed_queries"])
        else:
            row["failed_queries"] = []
        items.append(row)

    logger.debug("list_jobs returned %d rows (total=%d)", len(items), total)

    return items, total


def update_job_status(
    client: DatabricksClient,
    table: str,
    job_id: str,
    status: str,
    *,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    error: str | None = None,
    failed_queries: list[str] | None = None,
) -> None:
    """Update a job's status and optional timestamps."""
    sets = [f"status = '{_esc(status)}'"]

    if started_at is not None:
        sets.append(f"started_at = '{started_at.isoformat()}'")
    if completed_at is not None:
        sets.append(f"completed_at = '{completed_at.isoformat()}'")
    if error is not None:
        sets.append(f"error = '{_esc(error)}'")
    if failed_queries is not None:
        failed_queries_json = _esc(json.dumps(failed_queries))
        sets.append(f"failed_queries = '{failed_queries_json}'")

    sql = f"UPDATE {table} SET {', '.join(sets)} WHERE job_id = '{_esc(job_id)}'"
    client.execute_sql(sql)
