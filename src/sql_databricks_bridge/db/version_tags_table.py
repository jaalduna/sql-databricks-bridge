"""Databricks Delta table CRUD for Delta table version tags."""

import logging
from datetime import datetime
from typing import Any

from sql_databricks_bridge.db.databricks import DatabricksClient

logger = logging.getLogger(__name__)


def _esc(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    return value.replace("'", "''")


def ensure_version_tags_table(client: DatabricksClient, table: str) -> None:
    """Create the version_tags table if it does not exist."""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            table_name STRING NOT NULL,
            version BIGINT NOT NULL,
            tag STRING NOT NULL,
            description STRING,
            created_by STRING NOT NULL,
            created_at TIMESTAMP NOT NULL,
            job_id STRING
        ) USING DELTA
    """
    client.execute_sql(ddl)
    logger.info(f"Ensured version tags table exists: {table}")


def insert_version_tag(
    client: DatabricksClient,
    table: str,
    *,
    table_name: str,
    version: int,
    tag: str,
    created_by: str,
    description: str | None = None,
    job_id: str | None = None,
) -> None:
    """Insert a new version tag."""
    desc_val = f"'{_esc(description)}'" if description else "NULL"
    job_val = f"'{_esc(job_id)}'" if job_id else "NULL"
    now = datetime.utcnow().isoformat()

    sql = f"""
        INSERT INTO {table} (table_name, version, tag, description, created_by, created_at, job_id)
        VALUES (
            '{_esc(table_name)}',
            {version},
            '{_esc(tag)}',
            {desc_val},
            '{_esc(created_by)}',
            '{now}',
            {job_val}
        )
    """
    client.execute_sql(sql)


def list_version_tags(
    client: DatabricksClient,
    table: str,
    *,
    table_name: str | None = None,
    tag: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    """List version tags with optional filters. Returns (items, total_count)."""
    where_parts: list[str] = []
    if table_name:
        where_parts.append(f"table_name = '{_esc(table_name)}'")
    if tag:
        where_parts.append(f"tag = '{_esc(tag)}'")

    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

    count_rows = client.execute_sql(f"SELECT COUNT(*) AS cnt FROM {table}{where_sql}")
    total = int(count_rows[0]["cnt"]) if count_rows else 0

    rows = client.execute_sql(
        f"SELECT * FROM {table}{where_sql} ORDER BY created_at DESC LIMIT {limit} OFFSET {offset}"
    )

    return rows, total


def get_version_tag(
    client: DatabricksClient,
    table: str,
    *,
    table_name: str,
    tag: str,
) -> dict[str, Any] | None:
    """Get a specific version tag by table_name and tag. Returns None if not found."""
    sql = (
        f"SELECT * FROM {table} "
        f"WHERE table_name = '{_esc(table_name)}' AND tag = '{_esc(tag)}'"
    )
    rows = client.execute_sql(sql)
    return rows[0] if rows else None


def delete_version_tag(
    client: DatabricksClient,
    table: str,
    *,
    table_name: str,
    tag: str,
) -> bool:
    """Delete a version tag. Returns True if a row was found to delete."""
    existing = get_version_tag(client, table, table_name=table_name, tag=tag)
    if not existing:
        return False

    sql = (
        f"DELETE FROM {table} "
        f"WHERE table_name = '{_esc(table_name)}' AND tag = '{_esc(tag)}'"
    )
    client.execute_sql(sql)
    return True


def get_table_history(
    client: DatabricksClient,
    table_name: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Get Delta table version history via DESCRIBE HISTORY.

    Args:
        client: Databricks client.
        table_name: Fully-qualified table name (catalog.schema.table).
        limit: Max history entries to return.

    Returns:
        List of history entries with version, timestamp, operation, etc.
    """
    sql = f"DESCRIBE HISTORY {table_name} LIMIT {limit}"
    return client.execute_sql(sql)
