"""Databricks Delta table CRUD for traceability tags."""

import logging
from datetime import datetime
from typing import Any

from sql_databricks_bridge.db.databricks import DatabricksClient

logger = logging.getLogger(__name__)


def _esc(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    return value.replace("'", "''")


def ensure_traceability_tables(client: DatabricksClient, tags_table: str, tables_table: str) -> None:
    """Create the traceability_tags and traceability_tag_tables tables if they don't exist."""
    ddl_tags = f"""
        CREATE TABLE IF NOT EXISTS {tags_table} (
            tag_id       STRING NOT NULL,
            run_id       STRING NOT NULL,
            country      STRING NOT NULL,
            period       INT NOT NULL,
            stage        STRING NOT NULL,
            triggered_by STRING NOT NULL,
            started_at   TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            status       STRING NOT NULL,
            tables_count INT NOT NULL,
            created_at   TIMESTAMP NOT NULL
        ) USING DELTA
    """
    client.execute_sql(ddl_tags)
    logger.info(f"Ensured traceability tags table: {tags_table}")

    ddl_tables = f"""
        CREATE TABLE IF NOT EXISTS {tables_table} (
            tag_id      STRING NOT NULL,
            table_name  STRING NOT NULL,
            full_name   STRING NOT NULL,
            catalog     STRING NOT NULL,
            schema_name STRING NOT NULL,
            row_count   BIGINT NOT NULL,
            size_bytes  BIGINT,
            created_at  TIMESTAMP NOT NULL
        ) USING DELTA
    """
    client.execute_sql(ddl_tables)
    logger.info(f"Ensured traceability tag tables table: {tables_table}")


def insert_traceability_tag(
    client: DatabricksClient,
    tags_table: str,
    tables_table: str,
    *,
    tag_id: str,
    run_id: str,
    country: str,
    period: int,
    stage: str,
    triggered_by: str,
    started_at: str,
    completed_at: str | None,
    status: str,
    tables: list[dict[str, Any]],
) -> None:
    """Insert a new traceability tag with its associated tables."""
    now = datetime.utcnow().isoformat()
    tables_count = len(tables)
    completed_val = f"'{_esc(completed_at)}'" if completed_at else "NULL"

    sql_tag = f"""
        INSERT INTO {tags_table}
            (tag_id, run_id, country, period, stage, triggered_by, started_at, completed_at, status, tables_count, created_at)
        VALUES (
            '{_esc(tag_id)}',
            '{_esc(run_id)}',
            '{_esc(country)}',
            {period},
            '{_esc(stage)}',
            '{_esc(triggered_by)}',
            '{_esc(started_at)}',
            {completed_val},
            '{_esc(status)}',
            {tables_count},
            '{now}'
        )
    """
    client.execute_sql(sql_tag)

    for t in tables:
        size_val = str(t["size_bytes"]) if t.get("size_bytes") is not None else "NULL"
        sql_table = f"""
            INSERT INTO {tables_table}
                (tag_id, table_name, full_name, catalog, schema_name, row_count, size_bytes, created_at)
            VALUES (
                '{_esc(tag_id)}',
                '{_esc(t["table_name"])}',
                '{_esc(t["full_name"])}',
                '{_esc(t["catalog"])}',
                '{_esc(t["schema_name"])}',
                {int(t.get("row_count", 0))},
                {size_val},
                '{now}'
            )
        """
        client.execute_sql(sql_table)


def list_traceability_tags(
    client: DatabricksClient,
    tags_table: str,
    *,
    country: str | None = None,
    stage: str | None = None,
    period: int | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    """List traceability tags with optional filters. Returns (items, total_count)."""
    where_parts: list[str] = []
    if country:
        where_parts.append(f"country = '{_esc(country)}'")
    if stage:
        where_parts.append(f"stage = '{_esc(stage)}'")
    if period is not None:
        where_parts.append(f"period = {period}")
    if date_from:
        where_parts.append(f"created_at >= '{_esc(date_from)}'")
    if date_to:
        where_parts.append(f"created_at <= '{_esc(date_to)}'")

    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

    count_rows = client.execute_sql(f"SELECT COUNT(*) AS cnt FROM {tags_table}{where_sql}")
    total = int(count_rows[0]["cnt"]) if count_rows else 0

    rows = client.execute_sql(
        f"SELECT * FROM {tags_table}{where_sql} ORDER BY created_at DESC LIMIT {limit} OFFSET {offset}"
    )

    return rows, total


def get_traceability_tag(
    client: DatabricksClient,
    tags_table: str,
    *,
    tag_id: str,
) -> dict[str, Any] | None:
    """Get a single traceability tag by tag_id. Returns None if not found."""
    rows = client.execute_sql(
        f"SELECT * FROM {tags_table} WHERE tag_id = '{_esc(tag_id)}'"
    )
    return rows[0] if rows else None


def get_tag_tables(
    client: DatabricksClient,
    tables_table: str,
    *,
    tag_id: str,
) -> list[dict[str, Any]]:
    """Get all tables associated with a tag_id."""
    return client.execute_sql(
        f"SELECT * FROM {tables_table} WHERE tag_id = '{_esc(tag_id)}' ORDER BY table_name"
    )
