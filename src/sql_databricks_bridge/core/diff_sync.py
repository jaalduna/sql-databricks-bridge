"""Differential sync orchestrator.

Coordinates Level 1 fingerprint comparison and selective extraction:
  1. Compute Level 1 fingerprints (GROUP BY periodo), compare with stored
  2. Extract all rows for changed periods in a single query
  3. DELETE changed periods + INSERT new data in Databricks
  4. Update stored fingerprints

When ``two_phase=True`` the warehouse-touching steps (2-read, 3-write, 4-save)
are replaced with local SQLite operations + enqueueing, so the warehouse stays
OFF during the long extraction phase.
"""

from __future__ import annotations

import logging
import shutil
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Callable

import polars as pl

from sql_databricks_bridge.core.composite_columns import (
    is_composite,
    parse_columns,
    sql_server_where_in,
)
from sql_databricks_bridge.core.delta_writer import DeltaTableWriter
from sql_databricks_bridge.core.fingerprint import (
    compare_fingerprints,
    compute_level1_fingerprints,
    ensure_fingerprint_table,
    load_stored_fingerprints,
    save_fingerprints,
)
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.sql_server import SQLServerClient

if TYPE_CHECKING:
    from sql_databricks_bridge.core.fingerprint_cache import FingerprintCache
    from sql_databricks_bridge.core.sync_queue import SyncQueue

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
            from sql_databricks_bridge.core.transient_errors import (
                is_transient as _is_transient,
                is_lock_timeout as _is_lock_timeout,
                backoff_seconds as _backoff_seconds,
            )
            if _is_transient(err_str) and attempt < max_retries:
                delay = _backoff_seconds(err_str, attempt, base=base_delay)
                kind = "lock-contention" if _is_lock_timeout(err_str) else "transient"
                logger.warning(
                    f"{kind.capitalize()} SQL error (attempt {attempt+1}/{max_retries+1}), "
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
    queued: bool = False  # True when two-phase: write was enqueued, not committed


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
    fingerprint_only: bool = False,
    checksum_columns: list[str] | None = None,
    table_suffix: str | None = None,
    two_phase: bool = False,
    fingerprint_cache: FingerprintCache | None = None,
    sync_queue: SyncQueue | None = None,
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
        two_phase: If True, skip Databricks SQL and enqueue writes for Phase 2.
        fingerprint_cache: Local SQLite fingerprint cache (required when two_phase=True).
        sync_queue: Persistent queue for Databricks writes (required when two_phase=True).

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

    # Fingerprint key: suffix-aware so base table and suffixed table have
    # independent fingerprints (e.g. "vw_artigoz" vs "vw_artigoz_full").
    effective_fp_name = f"{sql_table}{table_suffix}" if table_suffix else sql_table

    def log_progress(msg: str) -> None:
        logger.info(f"[diff_sync:{country}/{sql_table}] {msg}")
        if on_progress:
            on_progress(msg)

    # Ensure fingerprint metadata table exists (skip in two-phase: no warehouse needed)
    if not two_phase:
        ensure_fingerprint_table(dbx_client, fingerprint_table)

    # --- LEVEL 1: Period-level comparison ---
    log_progress(f"Computing Level 1 fingerprints (GROUP BY {level1_column})")
    t0 = datetime.utcnow()
    current_l1 = compute_level1_fingerprints(sql_client, sql_table, level1_column, where_clause, base_query=base_query, checksum_columns=checksum_columns)
    stats.level1_fingerprint_time = (datetime.utcnow() - t0).total_seconds()
    stats.total_level1_values = len(current_l1)

    log_progress(f"Level 1: {len(current_l1)} values in {stats.level1_fingerprint_time:.1f}s")

    # Load stored Level 1 fingerprints
    if two_phase and fingerprint_cache is not None:
        # Two-phase: read from local SQLite cache (no warehouse)
        stored_l1 = fingerprint_cache.load(country, effective_fp_name, level="period")
        log_progress(f"Loaded {len(stored_l1)} stored fingerprints from local cache")
    else:
        stored_l1 = load_stored_fingerprints(
            dbx_client, fingerprint_table, country, effective_fp_name, level="period"
        )
    stats.is_first_sync = len(stored_l1) == 0

    # --- Schema compatibility check ---
    # If the Delta table exists but its schema doesn't match the source query,
    # force a full resync (OVERWRITE) to recreate the table with correct columns.
    # In two-phase mode we skip this (requires warehouse); Phase 2 handles it.
    _force_overwrite = False
    if not stats.is_first_sync and not two_phase:
        _target_table = writer.resolve_table_name(sql_table, country, table_suffix=table_suffix)
        if writer.table_exists(_target_table):
            _target_cols_list = writer._get_table_columns(_target_table)
            _target_cols = {c.lower() for c in _target_cols_list}
            if _target_cols:
                _needs_overwrite = False

                # Check 1: diff columns must exist in target for DELETE
                # Support composite level1 columns (e.g. "ano+mes+idproduto")
                _l1_parts = parse_columns(level1_column)
                _l1_ok = all(c.lower() in _target_cols for c in _l1_parts)
                _l2_ok = (not level2_column) or level2_column.lower() in _target_cols
                if not _l1_ok or not _l2_ok:
                    _missing = []
                    if not _l1_ok:
                        _missing.extend(c for c in _l1_parts if c.lower() not in _target_cols)
                    if not _l2_ok:
                        _missing.append(level2_column)
                    log_progress(
                        f"Schema mismatch: column(s) {_missing} not in Delta table"
                    )
                    _needs_overwrite = True

                # Check 2: source columns must match target for INSERT
                if not _needs_overwrite:
                    try:
                        _schema_sql = (
                            f"SELECT TOP 0 * FROM ({base_query}) AS _schema_check"
                            if base_query
                            else f"SELECT TOP 0 * FROM {sql_table}"
                        )
                        _schema_df = sql_client.execute_query(_schema_sql)
                        _source_cols = {c.lower() for c in _schema_df.columns}
                        _extra_in_target = _target_cols - _source_cols - {"_rescued_data"}
                        _extra_in_source = _source_cols - _target_cols
                        if _extra_in_target or _extra_in_source:
                            log_progress(
                                f"Schema mismatch: extra in Delta: "
                                f"{_extra_in_target or 'none'}, "
                                f"extra in source: {_extra_in_source or 'none'}"
                            )
                            _needs_overwrite = True
                    except Exception as e:
                        log_progress(f"Could not check source schema: {e}")

                if _needs_overwrite:
                    log_progress(
                        "Forcing full resync with OVERWRITE to fix schema"
                    )
                    stored_l1 = []
                    stats.is_first_sync = True
                    _force_overwrite = True
        else:
            # Target table doesn't exist but fingerprints do (e.g. table was
            # deleted, or using table_suffix for the first time).  Partial
            # diff-sync would create the table with only changed slices,
            # losing all "unchanged" rows.  Force a full extraction instead.
            log_progress(
                f"Target table {_target_table} does not exist but "
                f"fingerprints found — forcing full sync"
            )
            stored_l1 = []
            stats.is_first_sync = True
            _force_overwrite = True

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
        if is_composite(level1_column):
            log_progress(
                f"Mutable window ({mutable_months}mo) skipped: "
                f"not supported with composite level1 column '{level1_column}'"
            )
        else:
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

    # --- FINGERPRINT-ONLY MODE: save L1 and return without downloading data ---
    if fingerprint_only:
        log_progress("Fingerprint-only mode: saving Level 1 fingerprints and returning")
        if two_phase and fingerprint_cache is not None and sync_queue is not None:
            # Enqueue fingerprint save for Phase 2 + update local cache
            sync_queue.enqueue(
                job_id=job_id, country=country, table_name=effective_fp_name,
                operation="save_fingerprints",
                metadata={
                    "level": "period",
                    "fingerprints": [
                        {"value": fp.value, "row_count": fp.row_count, "checksum_xor": fp.checksum_xor}
                        for fp in current_l1
                    ],
                },
                tag=tag, table_suffix=table_suffix or "",
            )
            # Cache will be updated by Phase 2 after successful commit
            stats.queued = True
        else:
            save_fingerprints(
                dbx_client, fingerprint_table, country, effective_fp_name,
                level="period", fingerprints=current_l1,
                job_id=job_id,
            )
        stats.total_time = (datetime.utcnow() - total_start).total_seconds()
        return stats

    if not l1_diff.changed and not l1_diff.new:
        log_progress(
            f"No changes detected (unchanged={stats.unchanged_level1}, "
            f"out_of_scope={stats.deleted_level1}), skipping extraction"
        )
        # Still save fingerprints (narrowed to current scope) so next run
        # doesn't see stale entries for out-of-scope periods
        if stored_l1:
            if two_phase and fingerprint_cache is not None and sync_queue is not None:
                sync_queue.enqueue(
                    job_id=job_id, country=country, table_name=effective_fp_name,
                    operation="save_fingerprints",
                    metadata={
                        "level": "period",
                        "fingerprints": [
                            {"value": fp.value, "row_count": fp.row_count, "checksum_xor": fp.checksum_xor}
                            for fp in current_l1
                        ],
                    },
                    tag=tag, table_suffix=table_suffix or "",
                )
                # Cache will be updated by Phase 2 after successful commit
                stats.queued = True
            else:
                save_fingerprints(
                    dbx_client, fingerprint_table, country, effective_fp_name,
                    level="period", fingerprints=current_l1,
                    job_id=job_id,
                )
        stats.total_time = (datetime.utcnow() - total_start).total_seconds()
        return stats

    # --- Collect all periods that need syncing ---
    periods_to_sync = l1_diff.changed + l1_diff.new
    stats.total_changed_pairs = len(periods_to_sync)
    log_progress(f"Periods to sync: {len(periods_to_sync)} ({stats.changed_level1} changed, {stats.new_level1} new)")

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

    # --- EXTRACTION: Single query for all changed periods ---
    t0 = datetime.utcnow()
    if stats.is_first_sync or _force_overwrite:
        log_progress(f"Full extraction ({len(periods_to_sync)} periods)")
        if base_query:
            extract_query = base_query
        else:
            extract_query = f"SELECT * FROM {sql_table}"
            if where_clause:
                extract_query += f" WHERE {where_clause}"
    else:
        l1_cols = parse_columns(level1_column)
        where_filter = sql_server_where_in(l1_cols, periods_to_sync)
        extract_query = (
            f"SELECT * FROM {_sql_source} "
            f"WHERE {where_filter}"
        )

    bulk_dir = diff_chunk_dir / "bulk"
    all_chunks, total_rows = _extract_with_retry(
        sql_client, extract_query, chunk_size, output_dir=bulk_dir,
        on_progress=_report_rows,
    )
    stats.extraction_time = (datetime.utcnow() - t0).total_seconds()
    stats.rows_downloaded = total_rows
    log_progress(
        f"Extracted {stats.rows_downloaded:,} rows in {stats.extraction_time:.1f}s "
        f"(skipped {stats.rows_skipped:,} unchanged rows)"
    )

    # --- WRITE / ENQUEUE ---
    if two_phase and sync_queue is not None and fingerprint_cache is not None and all_chunks:
        # Two-phase: upload parquet to Volume (REST API, no warehouse) and enqueue
        log_progress("Two-phase: uploading parquet to Volume and enqueueing write")
        t0 = datetime.utcnow()

        from sql_databricks_bridge.core.config import get_settings
        _tp_settings = get_settings().databricks
        _tp_catalog = _tp_settings.catalog
        _tp_schema = country
        _tp_volume = _tp_settings.volume
        effective_name = f"{sql_table}{table_suffix}" if table_suffix else sql_table
        staging_dir = f"/Volumes/{_tp_catalog}/{_tp_schema}/{_tp_volume}/_staging_2p/{effective_name}_{job_id}"

        # Upload all chunks as numbered parquet parts
        chunks_lower = [c.rename({col: col.lower() for col in c.columns}) for c in all_chunks]
        writer.client.upload_dataframe_chunked(chunks_lower, staging_dir)
        log_progress(f"Uploaded {stats.rows_downloaded:,} rows to {staging_dir}")

        # Determine operation type
        if stats.is_first_sync or _force_overwrite:
            operation = "ctas"
            metadata = {}
        else:
            operation = "diff_write"
            metadata = {
                "periods_to_delete": periods_to_sync,
                "level1_column": level1_column,
                "level2_column": level2_column,
            }

        sync_queue.enqueue(
            job_id=job_id, country=country, table_name=sql_table,
            operation=operation, staging_path=staging_dir,
            metadata=metadata, tag=tag, table_suffix=table_suffix or "",
        )

        # Enqueue fingerprint save
        fp_metadata = {
            "level": "period",
            "fingerprints": [
                {"value": fp.value, "row_count": fp.row_count, "checksum_xor": fp.checksum_xor}
                for fp in current_l1
            ],
        }
        sync_queue.enqueue(
            job_id=job_id, country=country, table_name=effective_fp_name,
            operation="save_fingerprints", metadata=fp_metadata,
            tag=tag, table_suffix=table_suffix or "",
        )

        # Fingerprint cache will be updated by Phase 2 after successful commit

        stats.write_time = (datetime.utcnow() - t0).total_seconds()
        stats.queued = True

        log_progress(
            f"Two-phase: enqueued {operation} + fingerprints for {sql_table} "
            f"({stats.rows_downloaded:,} rows staged)"
        )
    else:
        # Standard single-phase: write directly to Databricks
        log_progress("Writing changed slices to Databricks")
        t0 = datetime.utcnow()

        if (stats.is_first_sync or _force_overwrite) and all_chunks:
            # First sync / schema fix: full OVERWRITE
            combined = pl.concat(all_chunks)
            result = writer.write_dataframe(combined, sql_table, country, tag=tag, table_suffix=table_suffix)
            log_progress(f"Wrote {result.rows} rows via OVERWRITE in {result.duration_seconds:.1f}s")
        elif all_chunks:
            # Incremental: DELETE changed periods + INSERT new data
            result = writer.write_diff_slices(
                chunks=all_chunks,
                query_name=sql_table,
                country=country,
                changed_pairs=[],
                level1_column=level1_column,
                level2_column=level2_column,
                deleted_level1_values=periods_to_sync,
                tag=tag,
                table_suffix=table_suffix,
            )
            log_progress(f"Wrote {result.rows} rows to {result.table_name} in {result.duration_seconds:.1f}s")

        stats.write_time = (datetime.utcnow() - t0).total_seconds()

        # --- UPDATE FINGERPRINTS: Save Level 1 only ---
        if stats.rows_downloaded > 0 or not stats.is_first_sync:
            save_fingerprints(
                dbx_client, fingerprint_table, country, effective_fp_name,
                level="period", fingerprints=current_l1,
                job_id=job_id,
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
        + (" (queued for Phase 2)" if stats.queued else "")
    )

    return stats
