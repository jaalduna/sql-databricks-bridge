"""SQLite-based local job store for trigger job persistence."""

import json
import logging
import os
import sqlite3
from typing import Any

logger = logging.getLogger(__name__)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS trigger_jobs (
    job_id TEXT PRIMARY KEY,
    country TEXT NOT NULL,
    stage TEXT NOT NULL,
    tag TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    queries TEXT NOT NULL,
    triggered_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    error TEXT,
    current_query TEXT,
    failed_queries TEXT DEFAULT '[]',
    results TEXT DEFAULT '[]',
    running_queries TEXT DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON trigger_jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created ON trigger_jobs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_triggered_by ON trigger_jobs(triggered_by);
"""

_MIGRATIONS = [
    "ALTER TABLE trigger_jobs ADD COLUMN running_queries TEXT DEFAULT '[]'",
]

# JSON fields that need deserialization when reading rows
_JSON_FIELDS = ("queries", "failed_queries", "results", "running_queries")


def _connect(db_path: str) -> sqlite3.Connection:
    """Open a connection with WAL mode and row-factory."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    """Convert a sqlite3.Row to a dict, deserializing JSON fields."""
    d = dict(row)
    for key in _JSON_FIELDS:
        if d.get(key) is not None:
            try:
                d[key] = json.loads(d[key])
            except (json.JSONDecodeError, TypeError):
                d[key] = [] if key != "queries" else []
        else:
            d[key] = []
    return d


def _run_migrations(conn: sqlite3.Connection) -> None:
    """Apply schema migrations for existing databases."""
    for sql in _MIGRATIONS:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass  # Column already exists
    conn.commit()


def init_db(db_path: str) -> str:
    """Create the database, tables, and indexes. Returns db_path."""
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = _connect(db_path)
    try:
        conn.executescript(_SCHEMA_SQL)
        _run_migrations(conn)
        conn.commit()
    finally:
        conn.close()
    logger.info(f"SQLite job store initialized: {db_path}")
    return db_path


def insert_job(
    db_path: str,
    *,
    job_id: str,
    country: str,
    stage: str,
    tag: str,
    queries: list[str],
    triggered_by: str,
    created_at: str,
) -> None:
    """Insert a new pending job."""
    conn = _connect(db_path)
    try:
        conn.execute(
            """INSERT INTO trigger_jobs
               (job_id, country, stage, tag, status, queries, triggered_by, created_at)
               VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)""",
            (
                job_id,
                country,
                stage,
                tag,
                json.dumps(queries),
                triggered_by,
                created_at,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def update_job(db_path: str, job_id: str, **fields: Any) -> None:
    """Update any subset of fields on a job."""
    if not fields:
        return
    set_clauses = []
    values = []
    for key, value in fields.items():
        set_clauses.append(f"{key} = ?")
        values.append(value)
    values.append(job_id)

    conn = _connect(db_path)
    try:
        conn.execute(
            f"UPDATE trigger_jobs SET {', '.join(set_clauses)} WHERE job_id = ?",
            values,
        )
        conn.commit()
    finally:
        conn.close()


def get_job(db_path: str, job_id: str) -> dict[str, Any] | None:
    """Get a single job by ID, deserializing JSON fields."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM trigger_jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def list_jobs(
    db_path: str,
    *,
    country: str | None = None,
    status: str | None = None,
    triggered_by: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    """Filtered + paginated job list. Returns (items, total_count)."""
    where_parts: list[str] = []
    params: list[Any] = []
    if country:
        where_parts.append("country = ?")
        params.append(country)
    if status:
        where_parts.append("status = ?")
        params.append(status)
    if triggered_by:
        where_parts.append("triggered_by = ?")
        params.append(triggered_by)

    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

    conn = _connect(db_path)
    try:
        total_row = conn.execute(
            f"SELECT COUNT(*) AS cnt FROM trigger_jobs{where_sql}", params
        ).fetchone()
        total = total_row["cnt"] if total_row else 0

        rows = conn.execute(
            f"SELECT * FROM trigger_jobs{where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()

        items = [_row_to_dict(r) for r in rows]
        return items, total
    finally:
        conn.close()


def mark_orphaned_jobs(db_path: str) -> int:
    """Mark pending/running jobs as failed on startup (crash recovery). Returns count."""
    conn = _connect(db_path)
    try:
        cursor = conn.execute(
            """UPDATE trigger_jobs
               SET status = 'failed',
                   error = 'Server restarted before job completed',
                   running_queries = '[]',
                   current_query = NULL
               WHERE status IN ('pending', 'running')""",
        )
        conn.commit()
        count = cursor.rowcount
        if count:
            logger.warning(f"Marked {count} orphaned job(s) as failed (crash recovery)")
        return count
    finally:
        conn.close()
