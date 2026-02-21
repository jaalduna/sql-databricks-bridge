"""Fingerprint computation and storage for differential sync.

Computes COUNT + CHECKSUM_AGG(CHECKSUM(*)) fingerprints on SQL Server at two levels:
  - Level 1: GROUP BY level1_column (e.g., periodo)
  - Level 2: GROUP BY level2_column for specific level1 values (e.g., idproduto)

Stores/retrieves fingerprints from a Databricks Delta metadata table for
cross-session comparison (SQL_now vs SQL_at_last_sync).
"""

import logging
from dataclasses import dataclass
from datetime import datetime

from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.sql_server import SQLServerClient

logger = logging.getLogger(__name__)

FINGERPRINT_TABLE_DDL = """\
CREATE TABLE IF NOT EXISTS {table} (
  country STRING,
  table_name STRING,
  level STRING,
  level1_value STRING,
  level2_value STRING,
  row_count BIGINT,
  checksum_xor BIGINT,
  synced_at TIMESTAMP,
  job_id STRING
) USING DELTA
"""


@dataclass(frozen=True)
class Fingerprint:
    """Single fingerprint entry (count + XOR checksum)."""
    value: str          # The grouped column value (e.g., '202401')
    row_count: int
    checksum_xor: int


@dataclass
class DiffResult:
    """Result of comparing current vs stored fingerprints."""
    changed: list[str]      # Values with different count or checksum
    new: list[str]          # Values present in SQL but not in stored
    deleted: list[str]      # Values present in stored but not in SQL
    unchanged: list[str]    # Values with matching count AND checksum

    @property
    def has_changes(self) -> bool:
        return bool(self.changed or self.new or self.deleted)

    @property
    def all_changed_values(self) -> list[str]:
        """All values that need re-sync (changed + new)."""
        return self.changed + self.new


# ---------------------------------------------------------------------------
# SQL Server fingerprint computation
# ---------------------------------------------------------------------------

def compute_level1_fingerprints(
    sql_client: SQLServerClient,
    table: str,
    level1_column: str,
    where_clause: str = "",
) -> list[Fingerprint]:
    """Compute Level 1 fingerprints: GROUP BY level1_column.

    Args:
        sql_client: SQL Server connection.
        table: Table name (e.g., 'j_atoscompra_new').
        level1_column: Column to group by (e.g., 'periodo').
        where_clause: Optional WHERE filter (e.g., 'periodo >= 202401').

    Returns:
        List of Fingerprint(value, row_count, checksum_xor).
    """
    where = f"WHERE {where_clause}" if where_clause else ""
    query = f"""
        SELECT
            CAST({level1_column} AS VARCHAR(100)) AS grp_value,
            COUNT(*) AS cnt,
            CHECKSUM_AGG(CHECKSUM(*)) AS chk
        FROM {table}
        {where}
        GROUP BY {level1_column}
        ORDER BY {level1_column}
    """
    logger.info(f"Computing Level 1 fingerprints: {table} GROUP BY {level1_column}")
    df = sql_client.execute_query(query)
    return [
        Fingerprint(
            value=str(row["grp_value"]).strip(),
            row_count=int(row["cnt"]),
            checksum_xor=int(row["chk"]),
        )
        for row in df.to_dicts()
    ]


def compute_level2_fingerprints(
    sql_client: SQLServerClient,
    table: str,
    level1_column: str,
    level1_value: str,
    level2_column: str,
) -> list[Fingerprint]:
    """Compute Level 2 fingerprints for a specific level1 value.

    Args:
        sql_client: SQL Server connection.
        table: Table name.
        level1_column: Level 1 column name (e.g., 'periodo').
        level1_value: Specific level 1 value (e.g., '202401').
        level2_column: Column to group by at level 2 (e.g., 'idproduto').

    Returns:
        List of Fingerprint(value, row_count, checksum_xor).
    """
    query = f"""
        SELECT
            CAST({level2_column} AS VARCHAR(100)) AS grp_value,
            COUNT(*) AS cnt,
            CHECKSUM_AGG(CHECKSUM(*)) AS chk
        FROM {table}
        WHERE {level1_column} = '{level1_value}'
        GROUP BY {level2_column}
        ORDER BY {level2_column}
    """
    logger.debug(f"Computing Level 2 fingerprints: {table} WHERE {level1_column}={level1_value} GROUP BY {level2_column}")
    df = sql_client.execute_query(query)
    return [
        Fingerprint(
            value=str(row["grp_value"]).strip(),
            row_count=int(row["cnt"]),
            checksum_xor=int(row["chk"]),
        )
        for row in df.to_dicts()
    ]


# ---------------------------------------------------------------------------
# Fingerprint comparison
# ---------------------------------------------------------------------------

def compare_fingerprints(
    current: list[Fingerprint],
    stored: list[Fingerprint],
) -> DiffResult:
    """Compare current fingerprints against stored ones.

    Args:
        current: Fresh fingerprints from SQL Server.
        stored: Previously stored fingerprints from last sync.

    Returns:
        DiffResult with changed, new, deleted, unchanged lists.
    """
    stored_map = {fp.value: fp for fp in stored}
    current_map = {fp.value: fp for fp in current}

    changed = []
    new = []
    unchanged = []

    for fp in current:
        prev = stored_map.get(fp.value)
        if prev is None:
            new.append(fp.value)
        elif fp.row_count != prev.row_count or fp.checksum_xor != prev.checksum_xor:
            changed.append(fp.value)
        else:
            unchanged.append(fp.value)

    deleted = [v for v in stored_map if v not in current_map]

    return DiffResult(
        changed=changed,
        new=new,
        deleted=deleted,
        unchanged=unchanged,
    )


# ---------------------------------------------------------------------------
# Fingerprint storage (Databricks Delta)
# ---------------------------------------------------------------------------

def ensure_fingerprint_table(dbx_client: DatabricksClient, table: str) -> None:
    """Create the fingerprint metadata table if it doesn't exist."""
    ddl = FINGERPRINT_TABLE_DDL.format(table=table)
    try:
        dbx_client.execute_sql(ddl)
        logger.info(f"Ensured fingerprint table exists: {table}")
    except Exception as e:
        logger.error(f"Failed to create fingerprint table {table}: {e}")
        raise


def load_stored_fingerprints(
    dbx_client: DatabricksClient,
    fingerprint_table: str,
    country: str,
    table_name: str,
    level: str,
    level1_value: str | None = None,
) -> list[Fingerprint]:
    """Load stored fingerprints from Databricks.

    Args:
        dbx_client: Databricks client.
        fingerprint_table: Fully-qualified fingerprint table name.
        country: Country code.
        table_name: Source table name.
        level: 'period' or 'product'.
        level1_value: If level='product', filter to this level1 value.

    Returns:
        List of stored Fingerprint objects.
    """
    where_parts = [
        f"country = '{country}'",
        f"table_name = '{table_name}'",
        f"level = '{level}'",
    ]
    if level1_value is not None:
        where_parts.append(f"level1_value = '{level1_value}'")

    where = " AND ".join(where_parts)
    query = f"""
        SELECT
            CASE WHEN level = 'period' THEN level1_value ELSE level2_value END AS grp_value,
            row_count,
            checksum_xor
        FROM {fingerprint_table}
        WHERE {where}
    """
    try:
        rows = dbx_client.execute_sql(query)
        return [
            Fingerprint(
                value=str(row["grp_value"]).strip(),
                row_count=int(row["row_count"]),
                checksum_xor=int(row["checksum_xor"]),
            )
            for row in rows
        ]
    except Exception as e:
        logger.warning(f"Failed to load stored fingerprints ({country}/{table_name}/{level}): {e}")
        return []


def save_fingerprints(
    dbx_client: DatabricksClient,
    fingerprint_table: str,
    country: str,
    table_name: str,
    level: str,
    fingerprints: list[Fingerprint],
    job_id: str,
    level1_value: str | None = None,
) -> None:
    """Save fingerprints to Databricks, replacing existing ones for this scope.

    Args:
        dbx_client: Databricks client.
        fingerprint_table: Fully-qualified fingerprint table name.
        country: Country code.
        table_name: Source table name.
        level: 'period' or 'product'.
        fingerprints: Fingerprints to store.
        job_id: Job ID for audit trail.
        level1_value: If level='product', the parent level1 value.
    """
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Delete existing fingerprints for this scope
    delete_where = [
        f"country = '{country}'",
        f"table_name = '{table_name}'",
        f"level = '{level}'",
    ]
    if level1_value is not None:
        delete_where.append(f"level1_value = '{level1_value}'")

    delete_sql = f"DELETE FROM {fingerprint_table} WHERE {' AND '.join(delete_where)}"

    try:
        dbx_client.execute_sql(delete_sql)
    except Exception as e:
        logger.warning(f"Failed to delete old fingerprints: {e}")

    # Insert new fingerprints in batches
    if not fingerprints:
        return

    batch_size = 100
    for i in range(0, len(fingerprints), batch_size):
        batch = fingerprints[i:i + batch_size]
        values = []
        for fp in batch:
            l1 = level1_value or fp.value if level == "period" else (level1_value or "")
            l2 = fp.value if level == "product" else "NULL"
            l2_sql = f"'{l2}'" if l2 != "NULL" else "NULL"
            values.append(
                f"('{country}', '{table_name}', '{level}', '{l1}', {l2_sql}, "
                f"{fp.row_count}, {fp.checksum_xor}, '{now}', '{job_id}')"
            )

        insert_sql = f"""
            INSERT INTO {fingerprint_table}
            (country, table_name, level, level1_value, level2_value,
             row_count, checksum_xor, synced_at, job_id)
            VALUES {', '.join(values)}
        """
        try:
            dbx_client.execute_sql(insert_sql)
        except Exception as e:
            logger.error(f"Failed to save fingerprint batch: {e}")
            raise

    logger.info(f"Saved {len(fingerprints)} {level}-level fingerprints for {country}/{table_name}")
