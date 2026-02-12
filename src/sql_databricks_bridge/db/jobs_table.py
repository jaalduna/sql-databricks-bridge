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
            error STRING
        ) USING DELTA
    """
    client.execute_sql(ddl)
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
) -> None:
    """Insert a new job record into the Delta table."""
    queries_json = _esc(json.dumps(queries))
    sql = f"""
        INSERT INTO {table} (job_id, country, stage, tag, status, queries, triggered_by, created_at)
        VALUES (
            '{_esc(job_id)}',
            '{_esc(country)}',
            '{_esc(stage)}',
            '{_esc(tag)}',
            'pending',
            '{queries_json}',
            '{_esc(triggered_by)}',
            '{created_at.isoformat()}'
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
    return row


def list_jobs(
    client: DatabricksClient,
    table: str,
    *,
    country: str | None = None,
    status: str | None = None,
    triggered_by: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    """List jobs with optional filters. Returns (items, total_count)."""
    where_parts: list[str] = []
    if country:
        where_parts.append(f"country = '{_esc(country)}'")
    if status:
        where_parts.append(f"status = '{_esc(status)}'")
    if triggered_by:
        where_parts.append(f"triggered_by = '{_esc(triggered_by)}'")

    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

    count_rows = client.execute_sql(f"SELECT COUNT(*) AS cnt FROM {table}{where_sql}")
    total = int(count_rows[0]["cnt"]) if count_rows else 0

    rows = client.execute_sql(
        f"SELECT * FROM {table}{where_sql} ORDER BY created_at DESC LIMIT {limit} OFFSET {offset}"
    )

    items = []
    for row in rows:
        if row.get("queries"):
            row["queries"] = json.loads(row["queries"])
        items.append(row)

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
) -> None:
    """Update a job's status and optional timestamps."""
    sets = [f"status = '{_esc(status)}'"]

    if started_at is not None:
        sets.append(f"started_at = '{started_at.isoformat()}'")
    if completed_at is not None:
        sets.append(f"completed_at = '{completed_at.isoformat()}'")
    if error is not None:
        sets.append(f"error = '{_esc(error)}'")

    sql = f"UPDATE {table} SET {', '.join(sets)} WHERE job_id = '{_esc(job_id)}'"
    client.execute_sql(sql)
