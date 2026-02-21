"""Differential sync orchestrator.

Coordinates the 2-level fingerprint comparison and selective extraction:
  1. Compute Level 1 fingerprints, compare with stored -> find changed level1 values
  2. For changed level1 values, compute Level 2 fingerprints -> find changed pairs
  3. Extract only changed (level1, level2) rows from SQL Server
  4. DELETE + INSERT changed slices in Databricks
  5. Update stored fingerprints
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterator

import polars as pl

from sql_databricks_bridge.core.delta_writer import DeltaTableWriter
from sql_databricks_bridge.core.fingerprint import (
    DiffResult,
    Fingerprint,
    compare_fingerprints,
    compute_level1_fingerprints,
    compute_level2_fingerprints,
    ensure_fingerprint_table,
    load_stored_fingerprints,
    save_fingerprints,
)
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.sql_server import SQLServerClient

logger = logging.getLogger(__name__)


@dataclass
class DiffSyncStats:
    """Statistics from a differential sync operation."""
    table_name: str
    country: str
    level1_column: str
    level2_column: str
    total_level1_values: int = 0
    changed_level1: int = 0
    new_level1: int = 0
    deleted_level1: int = 0
    unchanged_level1: int = 0
    total_changed_pairs: int = 0
    rows_downloaded: int = 0
    rows_skipped: int = 0
    is_first_sync: bool = False
    level1_fingerprint_time: float = 0
    level2_fingerprint_time: float = 0
    extraction_time: float = 0
    write_time: float = 0
    total_time: float = 0


def run_differential_sync(
    sql_client: SQLServerClient,
    dbx_client: DatabricksClient,
    writer: DeltaTableWriter,
    sql_table: str,
    country: str,
    level1_column: str,
    level2_column: str,
    fingerprint_table: str,
    where_clause: str = "",
    tag: str = "",
    job_id: str = "",
    chunk_size: int = 100_000,
    on_progress: callable = None,
) -> DiffSyncStats:
    """Run a full 2-level differential sync for a single table.

    Args:
        sql_client: SQL Server connection.
        dbx_client: Databricks client.
        writer: Delta table writer.
        sql_table: SQL Server table name (e.g., 'j_atoscompra_new').
        country: Country code (e.g., 'bolivia').
        level1_column: Column for Level 1 grouping (e.g., 'periodo').
        level2_column: Column for Level 2 grouping (e.g., 'idproduto').
        fingerprint_table: Databricks table for storing fingerprints.
        where_clause: SQL WHERE filter (e.g., 'periodo >= 202401').
        tag: Tag for the Delta table version.
        job_id: Job ID for audit trail.
        chunk_size: Rows per extraction chunk.
        on_progress: Optional callback(message: str) for progress updates.

    Returns:
        DiffSyncStats with detailed sync statistics.
    """
    stats = DiffSyncStats(
        table_name=sql_table,
        country=country,
        level1_column=level1_column,
        level2_column=level2_column,
    )
    total_start = datetime.utcnow()

    def log_progress(msg: str) -> None:
        logger.info(f"[diff_sync:{country}/{sql_table}] {msg}")
        if on_progress:
            on_progress(msg)

    # Ensure fingerprint metadata table exists
    ensure_fingerprint_table(dbx_client, fingerprint_table)

    # --- LEVEL 1: Period-level comparison ---
    log_progress(f"Computing Level 1 fingerprints (GROUP BY {level1_column})")
    t0 = datetime.utcnow()
    current_l1 = compute_level1_fingerprints(sql_client, sql_table, level1_column, where_clause)
    stats.level1_fingerprint_time = (datetime.utcnow() - t0).total_seconds()
    stats.total_level1_values = len(current_l1)

    log_progress(f"Level 1: {len(current_l1)} values in {stats.level1_fingerprint_time:.1f}s")

    # Load stored Level 1 fingerprints
    stored_l1 = load_stored_fingerprints(
        dbx_client, fingerprint_table, country, sql_table, level="period"
    )
    stats.is_first_sync = len(stored_l1) == 0

    if stats.is_first_sync:
        log_progress("First sync: no stored fingerprints, will do full download")

    # Compare
    l1_diff = compare_fingerprints(current_l1, stored_l1)
    stats.changed_level1 = len(l1_diff.changed)
    stats.new_level1 = len(l1_diff.new)
    stats.deleted_level1 = len(l1_diff.deleted)
    stats.unchanged_level1 = len(l1_diff.unchanged)

    total_rows_current = sum(fp.row_count for fp in current_l1)
    skipped_rows = sum(
        fp.row_count for fp in current_l1 if fp.value in set(l1_diff.unchanged)
    )
    stats.rows_skipped = skipped_rows

    log_progress(
        f"Level 1 diff: {stats.changed_level1} changed, {stats.new_level1} new, "
        f"{stats.deleted_level1} deleted, {stats.unchanged_level1} unchanged"
    )

    if not l1_diff.has_changes and not l1_diff.deleted:
        log_progress("No changes detected, skipping extraction")
        stats.total_time = (datetime.utcnow() - total_start).total_seconds()
        return stats

    # --- LEVEL 2: Product-level drill-down for changed level1 values ---
    all_changed_pairs: list[tuple[str, str]] = []
    all_new_pairs: list[tuple[str, str]] = []  # From new level1 values (all products new)

    # For NEW level1 values, we download everything (no Level 2 needed)
    for l1_val in l1_diff.new:
        log_progress(f"New {level1_column}={l1_val}: will download all products")
        # We'll extract all rows for this level1 value without Level 2 drill-down
        all_new_pairs.append((l1_val, "__ALL__"))

    # For CHANGED level1 values, drill down to Level 2
    if l1_diff.changed:
        log_progress(f"Drilling down Level 2 for {len(l1_diff.changed)} changed {level1_column} values")
        t0 = datetime.utcnow()

        for l1_val in l1_diff.changed:
            current_l2 = compute_level2_fingerprints(
                sql_client, sql_table, level1_column, l1_val, level2_column
            )

            stored_l2 = load_stored_fingerprints(
                dbx_client, fingerprint_table, country, sql_table,
                level="product", level1_value=l1_val,
            )

            l2_diff = compare_fingerprints(current_l2, stored_l2)

            for l2_val in l2_diff.all_changed_values:
                all_changed_pairs.append((l1_val, l2_val))

            # For deleted products in this period, we need to handle deletion
            for l2_val in l2_diff.deleted:
                all_changed_pairs.append((l1_val, l2_val))

            log_progress(
                f"  {level1_column}={l1_val}: {len(l2_diff.changed)} changed, "
                f"{len(l2_diff.new)} new, {len(l2_diff.deleted)} deleted, "
                f"{len(l2_diff.unchanged)} unchanged products"
            )

            # Save Level 2 fingerprints for this period
            save_fingerprints(
                dbx_client, fingerprint_table, country, sql_table,
                level="product", fingerprints=current_l2,
                job_id=job_id, level1_value=l1_val,
            )

        stats.level2_fingerprint_time = (datetime.utcnow() - t0).total_seconds()

    stats.total_changed_pairs = len(all_changed_pairs) + len(all_new_pairs)
    log_progress(f"Total slices to sync: {stats.total_changed_pairs}")

    # --- EXTRACTION: Download only changed rows ---
    log_progress("Extracting changed rows from SQL Server")
    t0 = datetime.utcnow()
    all_chunks: list[pl.DataFrame] = []

    # Extract changed (level1, level2) pairs
    for l1_val, l2_val in all_changed_pairs:
        query = (
            f"SELECT * FROM {sql_table} "
            f"WHERE CAST({level1_column} AS VARCHAR(100)) = '{l1_val}' "
            f"AND CAST({level2_column} AS VARCHAR(100)) = '{l2_val}'"
        )
        for chunk in sql_client.execute_query_chunked(query, chunk_size=chunk_size):
            all_chunks.append(chunk)

    # Extract new level1 values (full download for that value)
    for l1_val, _ in all_new_pairs:
        query = (
            f"SELECT * FROM {sql_table} "
            f"WHERE CAST({level1_column} AS VARCHAR(100)) = '{l1_val}'"
        )
        for chunk in sql_client.execute_query_chunked(query, chunk_size=chunk_size):
            all_chunks.append(chunk)

    stats.extraction_time = (datetime.utcnow() - t0).total_seconds()
    stats.rows_downloaded = sum(len(c) for c in all_chunks)

    log_progress(
        f"Extracted {stats.rows_downloaded:,} rows in {stats.extraction_time:.1f}s "
        f"(skipped {stats.rows_skipped:,} unchanged rows)"
    )

    # --- WRITE: DELETE + INSERT in Databricks ---
    log_progress("Writing changed slices to Databricks")
    t0 = datetime.utcnow()

    # Build the list of pairs for DELETE (exclude __ALL__ markers)
    delete_pairs = [(l1, l2) for l1, l2 in all_changed_pairs if l2 != "__ALL__"]
    # For new level1 values, add them as deleted_level1 for the writer
    # (though they shouldn't exist in Databricks yet, this is safe)
    new_l1_values = [l1 for l1, _ in all_new_pairs]

    if all_chunks or l1_diff.deleted or delete_pairs:
        result = writer.write_diff_slices(
            chunks=all_chunks,
            query_name=sql_table,
            country=country,
            changed_pairs=delete_pairs,
            level1_column=level1_column,
            level2_column=level2_column,
            deleted_level1_values=l1_diff.deleted + new_l1_values,
            tag=tag,
        )
        log_progress(f"Wrote {result.rows} rows to {result.table_name} in {result.duration_seconds:.1f}s")

    stats.write_time = (datetime.utcnow() - t0).total_seconds()

    # --- UPDATE FINGERPRINTS: Save Level 1 fingerprints ---
    save_fingerprints(
        dbx_client, fingerprint_table, country, sql_table,
        level="period", fingerprints=current_l1,
        job_id=job_id,
    )

    # Save Level 2 fingerprints for new level1 values
    for l1_val in l1_diff.new:
        new_l2 = compute_level2_fingerprints(
            sql_client, sql_table, level1_column, l1_val, level2_column
        )
        save_fingerprints(
            dbx_client, fingerprint_table, country, sql_table,
            level="product", fingerprints=new_l2,
            job_id=job_id, level1_value=l1_val,
        )

    stats.total_time = (datetime.utcnow() - total_start).total_seconds()
    log_progress(
        f"Differential sync complete in {stats.total_time:.1f}s: "
        f"{stats.rows_downloaded:,} downloaded, {stats.rows_skipped:,} skipped"
    )

    return stats
