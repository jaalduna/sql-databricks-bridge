"""Local SQLite cache for differential-sync fingerprints.

Mirrors the Databricks Delta fingerprint table so Phase 1 (download) can
compare fingerprints without starting the SQL warehouse.  After Phase 2
commits, the local cache and Databricks Delta stay in sync.

On cold start (empty cache), ``sync_from_databricks()`` loads existing
fingerprints from Databricks once.
"""

import logging
import os
import sqlite3
from datetime import datetime

from sql_databricks_bridge.core.fingerprint import Fingerprint

logger = logging.getLogger(__name__)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS fingerprint_cache (
    country TEXT NOT NULL,
    table_name TEXT NOT NULL,
    level TEXT NOT NULL,
    level1_value TEXT NOT NULL DEFAULT '',
    row_count INTEGER NOT NULL,
    checksum_xor INTEGER NOT NULL,
    synced_at TEXT,
    job_id TEXT,
    PRIMARY KEY (country, table_name, level, level1_value)
);
CREATE INDEX IF NOT EXISTS idx_fp_country_table
    ON fingerprint_cache(country, table_name);
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


class FingerprintCache:
    """Local SQLite mirror of the Databricks fingerprint Delta table."""

    def __init__(self, db_path: str = ".bridge_data/fingerprint_cache.db") -> None:
        self.db_path = db_path
        _init_db(db_path)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def load(
        self,
        country: str,
        table_name: str,
        level: str,
    ) -> list[Fingerprint]:
        """Load cached fingerprints (replaces ``load_stored_fingerprints``)."""
        conn = _connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT level1_value, row_count, checksum_xor "
                "FROM fingerprint_cache "
                "WHERE country = ? AND table_name = ? AND level = ?",
                (country, table_name, level),
            ).fetchall()
            return [
                Fingerprint(
                    value=row["level1_value"],
                    row_count=row["row_count"],
                    checksum_xor=row["checksum_xor"],
                )
                for row in rows
            ]
        finally:
            conn.close()

    def is_empty(self, country: str, table_name: str, level: str) -> bool:
        """Return True if no cached fingerprints exist for this scope.

        If *table_name* is empty, checks whether the country has **any**
        cached fingerprints (used for the cold-start check).
        """
        conn = _connect(self.db_path)
        try:
            if table_name:
                row = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM fingerprint_cache "
                    "WHERE country = ? AND table_name = ? AND level = ?",
                    (country, table_name, level),
                ).fetchone()
            else:
                # Broad check: any fingerprints for this country?
                row = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM fingerprint_cache "
                    "WHERE country = ?",
                    (country,),
                ).fetchone()
            return row["cnt"] == 0
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def save(
        self,
        country: str,
        table_name: str,
        level: str,
        fingerprints: list[Fingerprint],
        job_id: str = "",
    ) -> None:
        """Replace cached fingerprints for the given scope."""
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        conn = _connect(self.db_path)
        try:
            conn.execute(
                "DELETE FROM fingerprint_cache "
                "WHERE country = ? AND table_name = ? AND level = ?",
                (country, table_name, level),
            )
            if fingerprints:
                conn.executemany(
                    "INSERT INTO fingerprint_cache "
                    "(country, table_name, level, level1_value, "
                    " row_count, checksum_xor, synced_at, job_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            country,
                            table_name,
                            level,
                            fp.value,
                            fp.row_count,
                            fp.checksum_xor,
                            now,
                            job_id,
                        )
                        for fp in fingerprints
                    ],
                )
            conn.commit()
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Cold-start: populate from Databricks
    # ------------------------------------------------------------------

    def sync_from_databricks(
        self,
        dbx_client,
        fingerprint_table: str,
        country: str | None = None,
    ) -> int:
        """One-time download of fingerprints from Databricks Delta.

        Args:
            dbx_client: DatabricksClient instance.
            fingerprint_table: Fully-qualified Databricks table name.
            country: If given, only sync fingerprints for this country.

        Returns:
            Number of fingerprint rows cached.
        """
        where = f"WHERE country = '{country}'" if country else ""
        query = (
            f"SELECT country, table_name, level, level1_value, "
            f"row_count, checksum_xor, synced_at, job_id "
            f"FROM {fingerprint_table} {where}"
        )
        try:
            rows = dbx_client.execute_sql(query)
        except Exception as e:
            logger.warning(f"Failed to sync fingerprints from Databricks: {e}")
            return 0

        if not rows:
            return 0

        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        conn = _connect(self.db_path)
        try:
            # Clear existing entries for the scope being synced
            if country:
                conn.execute(
                    "DELETE FROM fingerprint_cache WHERE country = ?",
                    (country,),
                )
            else:
                conn.execute("DELETE FROM fingerprint_cache")

            conn.executemany(
                "INSERT OR REPLACE INTO fingerprint_cache "
                "(country, table_name, level, level1_value, "
                " row_count, checksum_xor, synced_at, job_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        r["country"],
                        r["table_name"],
                        r["level"],
                        r.get("level1_value", ""),
                        int(r["row_count"]),
                        int(r["checksum_xor"]),
                        str(r.get("synced_at", now)),
                        r.get("job_id", ""),
                    )
                    for r in rows
                ],
            )
            conn.commit()
            count = len(rows)
            logger.info(
                f"Synced {count} fingerprint(s) from Databricks"
                + (f" for country={country}" if country else "")
            )
            return count
        finally:
            conn.close()
