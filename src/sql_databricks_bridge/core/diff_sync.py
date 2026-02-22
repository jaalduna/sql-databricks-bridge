"""Differential sync orchestrator.

Coordinates the 2-level fingerprint comparison and selective extraction:
  1. Compute Level 1 fingerprints, compare with stored -> find changed level1 values
  2. For changed level1 values, compute Level 2 fingerprints -> find changed pairs
  3. Extract only changed (level1, level2) rows from SQL Server
  4. DELETE + INSERT changed slices in Databricks
  5. Update stored fingerprints
"""

import logging
import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterator

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


def _extract_with_retry(
    sql_client,
    query,
    chunk_size,
    max_retries=3,
    base_delay=30,
    output_dir: Path | None = None,
    on_progress: Callable[[int, int], None] | None = None,
) -> tuple[list[pl.DataFrame], int]:
    """Extract data to disk with retry on transient SQL Server errors.

    If output_dir is provided, streams chunks to parquet files on disk and
    returns (list of DataFrames read back from disk, total_rows).
    Otherwise falls back to in-memory extraction for backward compatibility.
    """
    for attempt in range(max_retries + 1):
        try:
            if output_dir is not None:
                # Clean up partial data from previous attempts
                if output_dir.exists():
                    shutil.rmtree(output_dir)
                parts, total_rows = sql_client.execute_query_to_disk(
                    query, output_dir, chunk_size=chunk_size, on_progress=on_progress,
                )
                if parts:
                    chunks = [pl.read_parquet(p) for p in parts]
                else:
                    chunks = []
                return chunks, total_rows
            else:
                chunks = []
                total = 0
                for chunk in sql_client.execute_query_chunked(query, chunk_size=chunk_size):
                    chunks.append(chunk)
                    total += len(chunk)
                    if on_progress:
                        on_progress(len(chunk), total)
                return chunks, total
        except Exception as e:
            err_str = str(e)
            is_transient = any(
                code in err_str
                for code in [
                    "08S01",
                    "08001",
                    "10054",
                    "10053",
                    "Communication link failure",
                    "connection was forcibly closed",
                ]
            )
            if is_transient and attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    f"Transient SQL error (attempt {attempt+1}/{max_retries+1}), "
                    f"retrying in {delay}s: {err_str[:200]}"
                )
                time.sleep(delay)
                continue
            raise


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
    on_progress: Callable | None = None,
    on_rows_progress: Callable[[int, int, int | None], None] | None = None,
    base_query: str | None = None,
    mutable_months: int = 0,
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
        on_rows_progress: Optional callback(chunk_rows, total_so_far, estimated_total) for row-level progress.
        mutable_months: If > 0, only check the last N months for changes (older periods are assumed unchanged).

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
    current_l1 = compute_level1_fingerprints(sql_client, sql_table, level1_column, where_clause, base_query=base_query)
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

    # Report estimated total rows to download (total minus unchanged)
    estimated_download = total_rows_current - skipped_rows
    if on_rows_progress:
        on_rows_progress(0, 0, estimated_download)

    log_progress(
        f"Level 1 diff: {stats.changed_level1} changed, {stats.new_level1} new, "
        f"{stats.deleted_level1} deleted, {stats.unchanged_level1} unchanged"
    )

    # --- MUTABLE WINDOW: Only process recent periods ---
    if mutable_months and not stats.is_first_sync:
        now = datetime.utcnow()
        # Compute cutoff period (YYYYMM format)
        cutoff_year = now.year
        cutoff_month = now.month - mutable_months
        while cutoff_month <= 0:
            cutoff_month += 12
            cutoff_year -= 1
        cutoff_str = f"{cutoff_year}{cutoff_month:02d}"

        def _in_window(val: str) -> bool:
            """Check if a period value (e.g. '202501') is within the mutable window."""
            try:
                return val >= cutoff_str
            except (TypeError, ValueError):
                return True  # Non-numeric values always checked

        orig_changed = len(l1_diff.changed)
        orig_new = len(l1_diff.new)
        l1_diff.changed = [v for v in l1_diff.changed if _in_window(v)]
        l1_diff.new = [v for v in l1_diff.new if _in_window(v)]
        skipped_by_window = (orig_changed - len(l1_diff.changed)) + (orig_new - len(l1_diff.new))
        if skipped_by_window:
            log_progress(
                f"Mutable window ({mutable_months}mo, cutoff={cutoff_str}): "
                f"checking {len(l1_diff.changed)} changed + {len(l1_diff.new)} new, "
                f"skipped {skipped_by_window} older periods"
            )
            # Recalculate estimated download
            window_values = set(l1_diff.changed + l1_diff.new)
            estimated_download = sum(
                fp.row_count for fp in current_l1 if fp.value in window_values
            )
            skipped_rows = total_rows_current - estimated_download
            stats.rows_skipped = skipped_rows
            if on_rows_progress:
                on_rows_progress(0, 0, estimated_download)

    if not l1_diff.changed and not l1_diff.new:
        log_progress(
            f"No changes detected (unchanged={stats.unchanged_level1}, "
            f"out_of_scope={stats.deleted_level1}), skipping extraction"
        )
        # Still save fingerprints (narrowed to current scope) so next run
        # doesn't see stale entries for out-of-scope periods
        if stored_l1:
            save_fingerprints(
                dbx_client, fingerprint_table, country, sql_table,
                level="period", fingerprints=current_l1,
                job_id=job_id,
            )
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
                sql_client, sql_table, level1_column, l1_val, level2_column,
                base_query=base_query,
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

    # --- FIRST SYNC OPTIMIZATION ---
    # If all L1 values are new and there are many, use a single full query
    use_bulk_extraction = stats.is_first_sync and len(all_new_pairs) > 10
    all_chunks: list[pl.DataFrame] = []
    _rows_so_far = 0  # Running total for progress tracking

    def _report_rows(chunk_rows: int, _total_ignored: int) -> None:
        """Track row-level progress during extraction."""
        nonlocal _rows_so_far
        _rows_so_far += chunk_rows
        if on_rows_progress:
            on_rows_progress(chunk_rows, _rows_so_far, estimated_download)

    # Create a temp directory for disk-streaming extraction
    diff_chunk_dir = Path(".bridge_data") / "chunks" / f"diff_{job_id}" / sql_table

    # Determine SQL source: use base_query as subquery if provided, else raw table
    _sql_source = f"({base_query}) AS _src" if base_query else sql_table

    if use_bulk_extraction:
        log_progress(f"First sync with {len(all_new_pairs)} periods: using bulk extraction")
        # Build a single query for all periods at once
        if base_query:
            bulk_query = base_query
        else:
            bulk_query = f"SELECT * FROM {sql_table}"
            if where_clause:
                bulk_query += f" WHERE {where_clause}"

        t0 = datetime.utcnow()
        bulk_dir = diff_chunk_dir / "bulk"
        all_chunks, bulk_total = _extract_with_retry(
            sql_client, bulk_query, chunk_size, output_dir=bulk_dir,
            on_progress=_report_rows,
        )
        stats.extraction_time = (datetime.utcnow() - t0).total_seconds()
        stats.rows_downloaded = bulk_total
        log_progress(
            f"Extracted {stats.rows_downloaded:,} rows in {stats.extraction_time:.1f}s (bulk)"
        )
    else:
        # --- EXTRACTION: Download only changed rows ---
        log_progress("Extracting changed rows from SQL Server")
        t0 = datetime.utcnow()

        # Extract changed (level1, level2) pairs
        for idx, (l1_val, l2_val) in enumerate(all_changed_pairs):
            query = (
                f"SELECT * FROM {_sql_source} "
                f"WHERE CAST({level1_column} AS VARCHAR(100)) = '{l1_val}' "
                f"AND CAST({level2_column} AS VARCHAR(100)) = '{l2_val}'"
            )
            pair_dir = diff_chunk_dir / f"changed_{idx}"
            chunks, _ = _extract_with_retry(
                sql_client, query, chunk_size, output_dir=pair_dir,
                on_progress=_report_rows,
            )
            all_chunks.extend(chunks)

        # Extract new level1 values (full download for that value)
        for i, (l1_val, _) in enumerate(all_new_pairs):
            query = (
                f"SELECT * FROM {_sql_source} "
                f"WHERE CAST({level1_column} AS VARCHAR(100)) = '{l1_val}'"
            )
            new_dir = diff_chunk_dir / f"new_{i}"
            chunks, _ = _extract_with_retry(
                sql_client, query, chunk_size, output_dir=new_dir,
                on_progress=_report_rows,
            )
            all_chunks.extend(chunks)
            slice_rows = sum(len(c) for c in chunks)
            log_progress(f"  [{i+1}/{len(all_new_pairs)}] periodo={l1_val}: {slice_rows:,} rows")

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

    if use_bulk_extraction and all_chunks:
        # First sync: full OVERWRITE is simpler and more reliable
        combined = pl.concat(all_chunks)
        result = writer.write_dataframe(combined, sql_table, country, tag=tag)
        log_progress(f"Wrote {result.rows} rows via OVERWRITE in {result.duration_seconds:.1f}s")
    elif all_chunks or delete_pairs:
        # Incremental: DELETE + INSERT changed slices
        # Note: l1_diff.deleted (periods outside current scope) are NOT deleted from
        # Databricks — they just fell outside the lookback window, not truly deleted.
        # Only new_l1_values are pre-cleared (safe: they shouldn't exist in target yet).
        result = writer.write_diff_slices(
            chunks=all_chunks,
            query_name=sql_table,
            country=country,
            changed_pairs=delete_pairs,
            level1_column=level1_column,
            level2_column=level2_column,
            deleted_level1_values=new_l1_values,
            tag=tag,
        )
        log_progress(f"Wrote {result.rows} rows to {result.table_name} in {result.duration_seconds:.1f}s")

    stats.write_time = (datetime.utcnow() - t0).total_seconds()

    # --- UPDATE FINGERPRINTS: Save Level 1 fingerprints ---
    # Only save fingerprints if we actually downloaded data (or this is an incremental sync)
    if stats.rows_downloaded > 0 or not stats.is_first_sync:
        save_fingerprints(
            dbx_client, fingerprint_table, country, sql_table,
            level="period", fingerprints=current_l1,
            job_id=job_id,
        )

        # Save Level 2 fingerprints for new level1 values
        for l1_val in l1_diff.new:
            new_l2 = compute_level2_fingerprints(
                sql_client, sql_table, level1_column, l1_val, level2_column,
                base_query=base_query,
            )
            save_fingerprints(
                dbx_client, fingerprint_table, country, sql_table,
                level="product", fingerprints=new_l2,
                job_id=job_id, level1_value=l1_val,
            )
    else:
        log_progress("WARNING: First sync produced 0 rows, NOT saving fingerprints")

    # Clean up temp disk chunks
    try:
        if diff_chunk_dir.exists():
            shutil.rmtree(diff_chunk_dir)
    except Exception:
        pass

    stats.total_time = (datetime.utcnow() - total_start).total_seconds()
    log_progress(
        f"Differential sync complete in {stats.total_time:.1f}s: "
        f"{stats.rows_downloaded:,} downloaded, {stats.rows_skipped:,} skipped"
    )

    return stats
