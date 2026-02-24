"""SQLite store for eligibility run persistence."""

import json
import logging
import os
import sqlite3
from typing import Any

logger = logging.getLogger(__name__)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS eligibility_runs (
    run_id TEXT PRIMARY KEY,
    country TEXT NOT NULL,
    period INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    parameters_json TEXT,
    phase1_metrics_json TEXT,
    created_by TEXT,
    created_at TEXT,
    updated_at TEXT,
    approved_by TEXT,
    approved_at TEXT,
    approval_comment TEXT,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS idx_eligibility_country ON eligibility_runs(country);
CREATE INDEX IF NOT EXISTS idx_eligibility_period ON eligibility_runs(period);
CREATE INDEX IF NOT EXISTS idx_eligibility_status ON eligibility_runs(status);
CREATE INDEX IF NOT EXISTS idx_eligibility_created_at ON eligibility_runs(created_at DESC);

CREATE TABLE IF NOT EXISTS eligibility_files (
    file_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    stage INTEGER NOT NULL,
    direction TEXT NOT NULL,
    filename TEXT NOT NULL,
    stored_path TEXT NOT NULL,
    file_size_bytes INTEGER,
    uploaded_by TEXT,
    created_at TEXT,
    FOREIGN KEY (run_id) REFERENCES eligibility_runs(run_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_eligibility_files_run ON eligibility_files(run_id);
"""

_DB_PATH_DEFAULT = "eligibility.db"


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    for field in ("parameters_json", "phase1_metrics_json", "steps_json"):
        raw = d.get(field)
        if raw is not None:
            try:
                d[field] = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                d[field] = None
    return d


def init_eligibility_db(db_path: str) -> str:
    """Create table and indexes. Returns db_path."""
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = _connect(db_path)
    try:
        conn.executescript(_SCHEMA_SQL)
        # Idempotent migration: add new columns to eligibility_runs
        cursor = conn.cursor()
        for col in [
            "current_step TEXT",
            "steps_json TEXT",
            "started_at TEXT",
            "completed_at TEXT",
        ]:
            try:
                cursor.execute(
                    f"ALTER TABLE eligibility_runs ADD COLUMN {col}"
                )
            except sqlite3.OperationalError:
                pass  # Column already exists
        conn.commit()
    finally:
        conn.close()
    logger.info(f"Eligibility SQLite store initialized: {db_path}")
    return db_path


def insert_run(
    db_path: str,
    *,
    run_id: str,
    country: str,
    period: int,
    status: str = "pending",
    parameters_json: dict | None = None,
    created_by: str | None = None,
    created_at: str,
    updated_at: str,
) -> None:
    conn = _connect(db_path)
    try:
        conn.execute(
            """INSERT INTO eligibility_runs
               (run_id, country, period, status, parameters_json,
                created_by, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                run_id,
                country,
                period,
                status,
                json.dumps(parameters_json) if parameters_json is not None else None,
                created_by,
                created_at,
                updated_at,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def update_run(db_path: str, run_id: str, **fields: Any) -> None:
    """Update any subset of fields on a run."""
    if not fields:
        return
    set_clauses = []
    values = []
    for key, value in fields.items():
        set_clauses.append(f"{key} = ?")
        values.append(value)
    values.append(run_id)

    conn = _connect(db_path)
    try:
        conn.execute(
            f"UPDATE eligibility_runs SET {', '.join(set_clauses)} WHERE run_id = ?",
            values,
        )
        conn.commit()
    finally:
        conn.close()


def get_run(db_path: str, run_id: str) -> dict[str, Any] | None:
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM eligibility_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def list_runs(
    db_path: str,
    *,
    country: str | None = None,
    period: int | None = None,
    status: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    """Filtered + paginated run list. Returns (items, total_count)."""
    where_parts: list[str] = []
    params: list[Any] = []

    if country is not None:
        where_parts.append("country = ?")
        params.append(country)
    if period is not None:
        where_parts.append("period = ?")
        params.append(period)
    if status is not None:
        where_parts.append("status = ?")
        params.append(status)

    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

    conn = _connect(db_path)
    try:
        total_row = conn.execute(
            f"SELECT COUNT(*) AS cnt FROM eligibility_runs{where_sql}", params
        ).fetchone()
        total = total_row["cnt"] if total_row else 0

        rows = conn.execute(
            f"SELECT * FROM eligibility_runs{where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()

        return [_row_to_dict(r) for r in rows], total
    finally:
        conn.close()


def delete_run(db_path: str, run_id: str) -> bool:
    """Delete a run by ID. Returns True if a row was deleted."""
    conn = _connect(db_path)
    try:
        cursor = conn.execute(
            "DELETE FROM eligibility_runs WHERE run_id = ?", (run_id,)
        )
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


# --- eligibility_files CRUD ---


def insert_file(
    db_path: str,
    *,
    file_id: str,
    run_id: str,
    stage: int,
    direction: str,
    filename: str,
    stored_path: str,
    file_size_bytes: int | None = None,
    uploaded_by: str | None = None,
    created_at: str,
) -> None:
    conn = _connect(db_path)
    try:
        conn.execute(
            """INSERT INTO eligibility_files
               (file_id, run_id, stage, direction, filename,
                stored_path, file_size_bytes, uploaded_by, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                file_id,
                run_id,
                stage,
                direction,
                filename,
                stored_path,
                file_size_bytes,
                uploaded_by,
                created_at,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_files_for_run(
    db_path: str, run_id: str, stage: int | None = None, direction: str | None = None
) -> list[dict[str, Any]]:
    """List files for a run, optionally filtered by stage and direction."""
    where_parts = ["run_id = ?"]
    params: list[Any] = [run_id]
    if stage is not None:
        where_parts.append("stage = ?")
        params.append(stage)
    if direction is not None:
        where_parts.append("direction = ?")
        params.append(direction)

    conn = _connect(db_path)
    try:
        rows = conn.execute(
            f"SELECT * FROM eligibility_files WHERE {' AND '.join(where_parts)} ORDER BY created_at",
            params,
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_file(db_path: str, file_id: str) -> dict[str, Any] | None:
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM eligibility_files WHERE file_id = ?", (file_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def delete_file(db_path: str, file_id: str) -> bool:
    conn = _connect(db_path)
    try:
        cursor = conn.execute(
            "DELETE FROM eligibility_files WHERE file_id = ?", (file_id,)
        )
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()
