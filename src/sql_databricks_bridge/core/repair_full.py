"""Repair `_full` Delta tables that have missing data due to fingerprint collision.

When diff-sync was run with `table_suffix='_full'` before suffix-aware fingerprints
were implemented, the base table's fingerprints caused diff-sync to skip "unchanged"
groups — writing only changed slices to the new `_full` table.

This module detects missing Level 1 values (e.g. sectors or periods) and backfills
them from SQL Server.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import polars as pl

from sql_databricks_bridge.core.country_query_loader import CountryAwareQueryLoader
from sql_databricks_bridge.core.delta_writer import DeltaTableWriter
from sql_databricks_bridge.core.fingerprint import (
    Fingerprint,
    compute_level1_fingerprints,
    save_fingerprints,
)
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.sql_server import SQLServerClient

logger = logging.getLogger(__name__)


@dataclass
class RepairResult:
    """Result of repairing a single table."""
    table_name: str
    country: str
    missing_values: list[str] = field(default_factory=list)
    rows_inserted: int = 0
    status: str = "ok"  # ok, repaired, error, dry_run
    error: str = ""
    duration_s: float = 0.0


def find_missing_level1_values(
    dbx_client: DatabricksClient,
    writer: DeltaTableWriter,
    country: str,
    sql_table: str,
    level1_col: str,
    table_suffix: str = "_full",
) -> list[str]:
    """Find Level 1 values present in the base table but missing in the suffixed table.

    Args:
        dbx_client: Databricks client.
        writer: Delta table writer (for resolve_table_name).
        country: Country code.
        sql_table: Base SQL table name (e.g. 'vw_artigoz').
        level1_col: Level 1 column (e.g. 'idsector' or 'periodo').
        table_suffix: Suffix for the target table (default '_full').

    Returns:
        List of Level 1 values present in base but missing from suffixed table.
    """
    base_table = writer.resolve_table_name(sql_table, country)
    full_table = writer.resolve_table_name(sql_table, country, table_suffix=table_suffix)

    # Check both tables exist
    if not writer.table_exists(base_table):
        logger.warning(f"Base table {base_table} does not exist, nothing to compare")
        return []
    if not writer.table_exists(full_table):
        logger.warning(f"Full table {full_table} does not exist, nothing to compare")
        return []

    query = f"""
        SELECT CAST(b.{level1_col} AS STRING) AS val
        FROM (SELECT DISTINCT {level1_col} FROM {base_table}) b
        LEFT ANTI JOIN (SELECT DISTINCT {level1_col} FROM {full_table}) f
        ON b.{level1_col} = f.{level1_col}
        ORDER BY val
    """
    try:
        rows = dbx_client.execute_sql(query)
        return [str(r["val"]).strip() for r in rows]
    except Exception as e:
        logger.error(f"Failed to find missing values for {country}/{sql_table}: {e}")
        raise


def repair_table(
    sql_client: SQLServerClient,
    dbx_client: DatabricksClient,
    writer: DeltaTableWriter,
    country: str,
    sql_table: str,
    level1_col: str,
    level2_col: str,
    table_suffix: str,
    query_sql: str,
    fingerprint_table: str,
    dry_run: bool = False,
    checksum_columns: list[str] | None = None,
) -> RepairResult:
    """Repair a single suffixed table by backfilling missing Level 1 slices.

    Args:
        sql_client: SQL Server connection.
        dbx_client: Databricks client.
        writer: Delta table writer.
        country: Country code.
        sql_table: Base SQL table name.
        level1_col: Level 1 column name.
        level2_col: Level 2 column name.
        table_suffix: Suffix (e.g. '_full').
        query_sql: Full SQL query (with placeholders already substituted).
        fingerprint_table: Databricks fingerprint table name.
        dry_run: If True, detect gaps but don't write.
        checksum_columns: Optional checksum column subset.

    Returns:
        RepairResult with details of what was repaired.
    """
    result = RepairResult(table_name=sql_table, country=country)
    t0 = datetime.utcnow()

    try:
        missing = find_missing_level1_values(
            dbx_client, writer, country, sql_table, level1_col, table_suffix,
        )
        result.missing_values = missing

        if not missing:
            result.status = "ok"
            logger.info(f"[repair:{country}/{sql_table}] No missing values, table is complete")
            result.duration_s = (datetime.utcnow() - t0).total_seconds()
            return result

        logger.info(
            f"[repair:{country}/{sql_table}] Found {len(missing)} missing {level1_col} values: "
            f"{missing[:10]}{'...' if len(missing) > 10 else ''}"
        )

        if dry_run:
            result.status = "dry_run"
            result.duration_s = (datetime.utcnow() - t0).total_seconds()
            return result

        # Extract missing slices from SQL Server
        missing_csv = ", ".join(f"'{v}'" for v in missing)
        extract_query = (
            f"SELECT * FROM ({query_sql}) AS _src "
            f"WHERE CAST({level1_col} AS VARCHAR(100)) IN ({missing_csv})"
        )

        logger.info(f"[repair:{country}/{sql_table}] Extracting {len(missing)} missing slices from SQL Server")
        df = sql_client.execute_query(extract_query)
        result.rows_inserted = len(df)

        if len(df) == 0:
            logger.warning(f"[repair:{country}/{sql_table}] SQL Server returned 0 rows for missing slices")
            result.status = "ok"
            result.duration_s = (datetime.utcnow() - t0).total_seconds()
            return result

        # Append to the suffixed table (INSERT INTO, not OVERWRITE)
        logger.info(f"[repair:{country}/{sql_table}] Appending {len(df):,} rows to {sql_table}{table_suffix}")
        write_result = writer.append_dataframe(
            df, sql_table, country, table_suffix=table_suffix,
        )
        logger.info(f"[repair:{country}/{sql_table}] Wrote {write_result.rows:,} rows to {write_result.table_name}")

        # Refresh fingerprints under the suffixed name
        effective_fp_name = f"{sql_table}{table_suffix}"
        logger.info(f"[repair:{country}/{sql_table}] Refreshing fingerprints for {effective_fp_name}")
        current_fps = compute_level1_fingerprints(
            sql_client, sql_table, level1_col, base_query=query_sql,
            checksum_columns=checksum_columns,
        )
        save_fingerprints(
            dbx_client, fingerprint_table, country, effective_fp_name,
            level="period", fingerprints=current_fps, job_id="repair",
        )

        result.status = "repaired"

    except Exception as e:
        result.status = "error"
        result.error = str(e)
        logger.error(f"[repair:{country}/{sql_table}] Error: {e}")

    result.duration_s = (datetime.utcnow() - t0).total_seconds()
    return result


def _substitute_query_placeholders(query_sql: str, lookback_months: int = 24) -> str:
    """Replace standard placeholders in a query string."""
    from dateutil.relativedelta import relativedelta

    now = datetime.utcnow()
    start = now - relativedelta(months=lookback_months)
    query_sql = query_sql.replace("{lookback_months}", str(lookback_months))
    query_sql = query_sql.replace("{start_period}", start.strftime("%Y%m"))
    query_sql = query_sql.replace("{end_period}", now.strftime("%Y%m"))
    query_sql = query_sql.replace("{start_year}", str(start.year))
    query_sql = query_sql.replace("{end_year}", str(now.year))
    query_sql = query_sql.replace("{start_date}", f"'{start.strftime('%Y-%m-01')}'")
    query_sql = query_sql.replace("{end_date}", f"'{now.strftime('%Y-%m-%d')}'")
    return query_sql


def parse_diff_sync_config(diff_sync_tables: str, checksum_columns_str: str = "") -> dict:
    """Parse DIFF_SYNC_TABLES and DIFF_SYNC_CHECKSUM_COLUMNS env vars into config dict.

    Returns:
        dict mapping table_name -> {"level1_column", "level2_column", "mutable_months", "checksum_columns"}
    """
    cfg: dict[str, dict] = {}
    for entry in (diff_sync_tables or "").split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":")
        tbl = parts[0].strip()
        if tbl:
            c = {
                "level1_column": parts[1].strip() if len(parts) > 1 else "periodo",
                "level2_column": parts[2].strip() if len(parts) > 2 else "idproduto",
            }
            if len(parts) > 3:
                c["mutable_months"] = int(parts[3].strip())
            cfg[tbl] = c

    for entry in (checksum_columns_str or "").split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":")
        tbl = parts[0].strip()
        if tbl and len(parts) > 1 and tbl in cfg:
            cols = [c.strip() for c in parts[1].split("+") if c.strip()]
            if cols:
                cfg[tbl]["checksum_columns"] = cols

    return cfg


def repair_country(
    sql_client: SQLServerClient,
    dbx_client: DatabricksClient,
    writer: DeltaTableWriter,
    country: str,
    diff_tables_config: dict,
    queries_path: str,
    fingerprint_table: str,
    table_suffix: str = "_full",
    dry_run: bool = False,
    lookback_months: int = 24,
    only_table: str | None = None,
) -> list[RepairResult]:
    """Repair all (or one) suffixed tables for a country.

    Args:
        sql_client: SQL Server connection for this country.
        dbx_client: Databricks client.
        writer: Delta table writer.
        country: Country code.
        diff_tables_config: Parsed diff-sync config (from parse_diff_sync_config).
        queries_path: Path to queries directory.
        fingerprint_table: Databricks fingerprint table name.
        table_suffix: Suffix to repair (default '_full').
        dry_run: If True, report gaps without writing.
        lookback_months: Lookback months for query placeholders.
        only_table: If set, only repair this specific table.

    Returns:
        List of RepairResult, one per table.
    """
    loader = CountryAwareQueryLoader(queries_path)
    results: list[RepairResult] = []

    for table_name, config in diff_tables_config.items():
        if only_table and table_name != only_table:
            continue

        level1_col = config.get("level1_column", "periodo")
        level2_col = config.get("level2_column", "idproduto")
        checksum_cols = config.get("checksum_columns")

        # Load query SQL
        try:
            query_sql = loader.get_query(table_name, country)
            query_sql = _substitute_query_placeholders(query_sql, lookback_months)
        except Exception as e:
            logger.warning(f"[repair:{country}/{table_name}] Could not load query: {e}")
            results.append(RepairResult(
                table_name=table_name, country=country,
                status="error", error=f"Query not found: {e}",
            ))
            continue

        result = repair_table(
            sql_client=sql_client,
            dbx_client=dbx_client,
            writer=writer,
            country=country,
            sql_table=table_name,
            level1_col=level1_col,
            level2_col=level2_col,
            table_suffix=table_suffix,
            query_sql=query_sql,
            fingerprint_table=fingerprint_table,
            dry_run=dry_run,
            checksum_columns=checksum_cols,
        )
        results.append(result)

    return results
