"""Persistent queue for two-phase sync Databricks write operations.

Phase 1 (download) enqueues work items here.  Phase 2 (commit) drains
the queue, executing the Databricks SQL operations in a tight batch so
the SQL warehouse is only ON for a few minutes.
"""

import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sync_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    country TEXT NOT NULL,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    staging_path TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    committed_at TEXT,
    error TEXT,
    tag TEXT NOT NULL DEFAULT '',
    table_suffix TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_sq_status ON sync_queue(status);
CREATE INDEX IF NOT EXISTS idx_sq_job ON sync_queue(job_id);
"""


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _init_db(db_path: str) -> None:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = _connect(db_path)
    try:
        conn.executescript(_SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    raw = d.get("metadata_json")
    if raw:
        try:
            d["metadata"] = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            d["metadata"] = {}
    else:
        d["metadata"] = {}
    return d


class SyncQueue:
    """SQLite-backed queue for pending Databricks write operations."""

    def __init__(self, db_path: str = ".bridge_data/sync_queue.db") -> None:
        self.db_path = db_path
        _init_db(db_path)

    def enqueue(
        self,
        *,
        job_id: str,
        country: str,
        table_name: str,
        operation: str,
        staging_path: str = "",
        metadata: dict | None = None,
        tag: str = "",
        table_suffix: str = "",
    ) -> int:
        """Add a pending write operation to the queue.

        Args:
            job_id: Parent trigger job ID.
            country: Country code.
            table_name: Source SQL table name.
            operation: One of 'ctas', 'diff_write', 'save_fingerprints'.
            staging_path: Volume path to staged parquet directory.
            metadata: Operation-specific data (periods_to_delete, fingerprints, etc.).
            tag: Delta table tag.
            table_suffix: Optional table name suffix.

        Returns:
            The queue item ID.
        """
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        conn = _connect(self.db_path)
        try:
            cursor = conn.execute(
                "INSERT INTO sync_queue "
                "(job_id, country, table_name, operation, staging_path, "
                " metadata_json, status, created_at, tag, table_suffix) "
                "VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)",
                (
                    job_id,
                    country,
                    table_name,
                    operation,
                    staging_path,
                    json.dumps(metadata or {}),
                    now,
                    tag,
                    table_suffix,
                ),
            )
            conn.commit()
            return cursor.lastrowid  # type: ignore[return-value]
        finally:
            conn.close()

    def get_pending(self, job_id: str | None = None) -> list[dict[str, Any]]:
        """Return all pending queue items, optionally filtered by job_id."""
        conn = _connect(self.db_path)
        try:
            if job_id:
                rows = conn.execute(
                    "SELECT * FROM sync_queue WHERE status = 'pending' AND job_id = ? "
                    "ORDER BY id",
                    (job_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM sync_queue WHERE status = 'pending' ORDER BY id"
                ).fetchall()
            return [_row_to_dict(r) for r in rows]
        finally:
            conn.close()

    def mark_committed(self, queue_id: int) -> None:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        conn = _connect(self.db_path)
        try:
            conn.execute(
                "UPDATE sync_queue SET status = 'committed', committed_at = ? WHERE id = ?",
                (now, queue_id),
            )
            conn.commit()
        finally:
            conn.close()

    def mark_failed(self, queue_id: int, error: str) -> None:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        conn = _connect(self.db_path)
        try:
            conn.execute(
                "UPDATE sync_queue SET status = 'failed', committed_at = ?, error = ? "
                "WHERE id = ?",
                (now, error, queue_id),
            )
            conn.commit()
        finally:
            conn.close()

    def cleanup_old(self, days: int = 7) -> int:
        """Delete committed/failed items older than *days*. Returns count deleted."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        conn = _connect(self.db_path)
        try:
            cursor = conn.execute(
                "DELETE FROM sync_queue WHERE status IN ('committed', 'failed') "
                "AND created_at < ?",
                (cutoff,),
            )
            conn.commit()
            return cursor.rowcount
        finally:
            conn.close()

    def count_pending(self, job_id: str | None = None) -> int:
        conn = _connect(self.db_path)
        try:
            if job_id:
                row = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM sync_queue "
                    "WHERE status = 'pending' AND job_id = ?",
                    (job_id,),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM sync_queue WHERE status = 'pending'"
                ).fetchone()
            return row["cnt"] if row else 0
        finally:
            conn.close()
