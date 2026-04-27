"""Trigger API endpoints -- frontend-facing sync trigger and event views."""

import csv
import io
import json
import logging
import os
import re
import shutil
import threading
import time
import uuid as _uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from sql_databricks_bridge.api.dependencies import CurrentAzureADUser
from sql_databricks_bridge.api.schemas import JobStatus, QueryResult
from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.core.delta_writer import DeltaTableWriter
from sql_databricks_bridge.core.country_query_loader import CountryAwareQueryLoader
from sql_databricks_bridge.core.extractor import Extractor, ExtractionJob, concat_chunks
from sql_databricks_bridge.core.stages import build_tag
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db import local_store
from sql_databricks_bridge.db.jobs_table import (
    get_job,
    insert_job,
    list_jobs,
    update_job_status,
)
from sql_databricks_bridge.db.sql_server import SQLServerClient
from sql_databricks_bridge.db.version_tags_table import insert_version_tag
from sql_databricks_bridge.core.calibration_tracker import calibration_tracker
from sql_databricks_bridge.core.diff_sync import run_differential_sync
from sql_databricks_bridge.core.fingerprint_cache import FingerprintCache
from sql_databricks_bridge.core.sync_queue import SyncQueue
from sql_databricks_bridge.core.phase2_executor import Phase2Executor

logger = logging.getLogger(__name__)

# Module-level launcher reference, set by main.py at startup.
_calibration_launcher = None
# Module-level SQLite db path, set during first trigger request.
_sqlite_db_path: str | None = None

router = APIRouter(tags=["Trigger"])


# --- Schemas ---


class AggregationOptions(BaseModel):
    region: bool = Field(default=False, description="Include region-level aggregation")
    nivel_2: bool = Field(default=False, description="Include nivel_2 aggregation")


class DiffSyncQueryConfig(BaseModel):
    """Configuration for differential sync on a specific query/table."""
    level1_column: str = Field(default="periodo", description="GROUP BY column for Level 1 (e.g., 'periodo')")
    level2_column: str = Field(default="", description="GROUP BY column for Level 2 (e.g., 'idproduto'). Empty = level1-only mode.")
    where_clause: str = Field(default="", description="Optional WHERE filter for fingerprint scope")
    checksum_columns: list[str] | None = Field(default=None, description="Columns for CHECKSUM (instead of *). Faster for wide tables.")


class TriggerRequest(BaseModel):
    country: str = Field(..., description="Country code (e.g. 'bolivia', 'brazil')")
    stage: str = Field(..., description="Stage code (e.g. 'calibracion', 'mtr')")
    period: str | None = Field(
        default=None,
        description="Period code (e.g. '202602'). Stored with the job for filtering.",
    )
    queries: list[str] | None = Field(
        default=None,
        description="Specific queries to run. null = all queries for the country.",
    )
    aggregations: AggregationOptions | None = Field(
        default=None,
        description="Aggregation options for calibration (region, nivel_2).",
    )
    row_limit: int | None = Field(
        default=None,
        description="Override row limit per query (TOP N). null = use server default.",
    )
    lookback_months: int | None = Field(
        default=None,
        description="Override rolling lookback months. null = use server default.",
    )
    skip_sync: bool = Field(
        default=False,
        description="Skip the sync_data step (SQL Server extraction). Use when data is already synced.",
    )
    skip_copy: bool = Field(
        default=False,
        description="Skip the copy_to_calibration step (Bronze Copy). Use when data is already in the calibration catalog.",
    )
    differential_sync: dict[str, DiffSyncQueryConfig] | None = Field(
        default=None,
        description=(
            "Enable differential sync per query. Keys are query names, values "
            "configure the grouping columns. Queries not listed use full OVERWRITE. "
            "Example: {'j_atoscompra_new': {'level1_column': 'periodo', 'level2_column': 'idproduto'}}"
        ),
    )
    force_full_sync: bool = Field(
        default=False,
        description="Force full OVERWRITE for all queries, bypassing differential and incremental sync.",
    )
    fingerprint_only: bool = Field(
        default=False,
        description="Compute and store fingerprints only, without downloading any data.",
    )
    table_suffix: str | None = Field(
        default=None,
        description=(
            "Suffix appended to Delta table names (e.g. '_full' -> j_atoscompra_new_full). "
            "Allows writing to separate tables without overwriting the default ones."
        ),
    )
    suffix_exclude: list[str] | None = Field(
        default=None,
        description=(
            "Tables to exclude from table_suffix (synced without suffix). "
            "E.g. ['prediccion_compras'] syncs that table as-is while others get the suffix."
        ),
    )
    queries_path: str | None = Field(
        default=None,
        description="Override the server QUERIES_PATH for this request. Use a different queries directory.",
    )
    skip_phase2: bool = Field(
        default=False,
        description=(
            "When True and TWO_PHASE_SYNC is enabled, skip the Phase 2 commit "
            "after extraction. Items stay in the SyncQueue for a later batch commit "
            "via POST /api/v1/phase2/execute."
        ),
    )


class TriggerResponse(BaseModel):
    job_id: str
    status: str
    country: str
    stage: str
    period: str | None = None
    tag: str
    queries: list[str]
    queries_count: int
    created_at: datetime
    triggered_by: str


class DatabricksTaskStatusResponse(BaseModel):
    """Per-task progress within a Databricks multi-task job run."""

    task_key: str
    status: str = "pending"
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None


class CalibrationStepResponse(BaseModel):
    """Calibration step as expected by the calibration frontend."""

    name: str
    status: str
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None
    tasks: list[DatabricksTaskStatusResponse] = Field(default_factory=list)


class EventSummary(BaseModel):
    job_id: str
    status: JobStatus
    country: str
    stage: str
    period: str | None = None
    tag: str
    queries_total: int
    queries_completed: int
    queries_failed: int
    queries_running: int = 0
    running_queries: list[str] = Field(default_factory=list)
    failed_queries: list[str] = Field(default_factory=list)
    total_rows_extracted: int = 0
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    triggered_by: str
    error: str | None = None
    current_query: str | None = None
    steps: list[CalibrationStepResponse] | None = None
    current_step: str | None = None


class EventDetail(EventSummary):
    results: list[QueryResult] = Field(default_factory=list)


class EventListResponse(BaseModel):
    items: list[EventSummary]
    total: int
    limit: int
    offset: int


# --- Internal record ---


class _TriggerJobRecord:
    """Internal record linking a trigger to an extraction job."""

    def __init__(
        self,
        extractor: Extractor,
        job: ExtractionJob,
        triggered_by: str,
        stage: str = "",
        tag: str = "",
        period: str | None = None,
        aggregations: AggregationOptions | None = None,
    ) -> None:
        self.extractor = extractor
        self.job = job
        self.triggered_by = triggered_by
        self.stage = stage
        self.tag = tag
        self.period = period
        self.aggregations = aggregations
        self.running_queries: set[str] = set()  # live set updated by background threads


# Fix forward reference for module-level dict
_trigger_jobs: dict[str, _TriggerJobRecord] = {}


def _persist_steps(job_id: str, db_path: str) -> None:
    """Read current calibration steps from tracker and persist to SQLite."""
    steps_data = calibration_tracker.get_steps_for_response(job_id)
    if steps_data is None:
        return
    try:
        local_store.update_steps(db_path, job_id, steps_data)
    except Exception as exc:
        logger.warning("Failed to persist steps for job %s: %s", job_id, exc)


def finalize_trigger_job(
    job_id: str, status: str, completed_at: datetime, error: str | None = None
) -> None:
    """Called by DatabricksJobMonitor when all calibration steps are done.

    Updates the in-memory trigger record and persists to SQLite so that the
    frontend polling endpoint sees the correct final status.
    """
    record = _trigger_jobs.get(job_id)
    if record:
        record.job.status = JobStatus(status)
        record.job.completed_at = completed_at
        if error:
            record.job.error = error
        logger.info("Finalized in-memory trigger job %s → %s", job_id, status)

    db_path = _sqlite_db_path
    if db_path:
        try:
            local_store.update_job(
                db_path,
                job_id,
                status=status,
                completed_at=completed_at.isoformat(),
                error=error,
            )
            _persist_steps(job_id, db_path)
        except Exception as exc:
            logger.warning("Failed to finalize job %s in SQLite: %s", job_id, exc)


def _build_steps_for_job(
    job_id: str, db_path: str | None = None
) -> tuple[list[CalibrationStepResponse] | None, str | None]:
    """Get calibration steps and current_step for a job.

    Tries calibration_tracker first (in-memory, fast). Falls back to SQLite
    steps_json when the tracker has no data (e.g., after a server restart).
    """
    steps_data = calibration_tracker.get_steps_for_response(job_id)
    current = calibration_tracker.get_current_step(job_id)
    if steps_data is None and db_path:
        # Cold-read from SQLite after restart
        try:
            row = local_store.get_job(db_path, job_id)
            if row and row.get("steps_json"):
                steps_data = row["steps_json"]
                # Derive current step: last running step, or None if all settled
                current = next(
                    (s["name"] for s in steps_data if s.get("status") == "running"),
                    None,
                )
        except Exception as exc:
            logger.warning("Failed to read steps_json from SQLite for job %s: %s", job_id, exc)
    if steps_data is None:
        return None, None
    steps = [CalibrationStepResponse(**s) for s in steps_data]
    return steps, current


def _get_db_path(request: Request) -> str | None:
    """Get SQLite database path from app.state."""
    return getattr(request.app.state, "sqlite_db_path", None)


def _results_from_row(row: dict) -> list[QueryResult]:
    """Deserialize results list from a SQLite row."""
    raw = row.get("results", [])
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return []
    results = []
    for r in raw:
        if isinstance(r, dict):
            try:
                results.append(QueryResult(**r))
            except Exception:
                continue
    return results


def _compute_counts(results: list[QueryResult]) -> tuple[int, int]:
    """Count completed and failed queries from results list."""
    completed = sum(1 for r in results if r.status == JobStatus.COMPLETED)
    failed = sum(1 for r in results if r.status == JobStatus.FAILED)
    return completed, failed


def _total_rows(results: list[QueryResult]) -> int:
    """Sum rows_extracted across all results."""
    return sum(r.rows_extracted or 0 for r in results)


def _build_event_summary(row: dict, results: list[QueryResult]) -> EventSummary:
    """Build an EventSummary from a SQLite row and its deserialized results."""
    completed, failed = _compute_counts(results)
    running_queries = row.get("running_queries", [])
    if isinstance(running_queries, str):
        try:
            running_queries = json.loads(running_queries)
        except (json.JSONDecodeError, TypeError):
            running_queries = []
    queries = row.get("queries", [])
    if isinstance(queries, str):
        try:
            queries = json.loads(queries)
        except (json.JSONDecodeError, TypeError):
            queries = []
    steps, current_step = _build_steps_for_job(row["job_id"])
    return EventSummary(
        job_id=row["job_id"],
        status=row["status"],
        country=row["country"],
        stage=row["stage"],
        period=row.get("period"),
        tag=row["tag"],
        queries_total=len(queries),
        queries_completed=completed,
        queries_failed=failed,
        queries_running=len(running_queries),
        running_queries=running_queries,
        failed_queries=[r.query_name for r in results if r.status == "failed"],
        total_rows_extracted=_total_rows(results),
        created_at=row["created_at"],
        started_at=row.get("started_at"),
        completed_at=row.get("completed_at"),
        triggered_by=row["triggered_by"],
        error=row.get("error"),
        current_query=row.get("current_query"),
        steps=steps,
        current_step=current_step,
    )


def _launch_calibration_step(
    job_id: str,
    step_name: str,
    country: str,
    period: str | None = None,
    aggregations: dict[str, bool] | None = None,
) -> None:
    """Launch a Databricks job for a calibration step if a launcher is available."""
    if _calibration_launcher is None:
        logger.debug("No calibration launcher configured; skipping launch for %s/%s", job_id, step_name)
        return
    extra_params: dict[str, str] = {}
    if period:
        extra_params["final_period"] = period
    if aggregations:
        for key, value in aggregations.items():
            if value:
                extra_params[key] = "true"
    try:
        _calibration_launcher.launch_step(
            job_id, step_name, country, extra_params=extra_params or None,
        )
    except Exception as exc:
        logger.error("Failed to launch step %s for job %s: %s", step_name, job_id, exc)


# --- Background task ---


def _get_databricks_client(request: Request) -> DatabricksClient:
    """Get DatabricksClient from app.state, or create a new one as fallback."""
    client = getattr(request.app.state, "databricks_client", None)
    if client is not None:
        return client
    return DatabricksClient()


def _run_trigger_extraction(
    extractor: Extractor,
    job: ExtractionJob,
    writer: DeltaTableWriter,
    client: DatabricksClient,
    table: str,
    tag: str = "",
    triggered_by: str = "",
    db_path: str | None = None,
    max_parallel: int = 4,
    override_row_limit: int | None = None,
    override_lookback_months: int | None = None,
    differential_sync_config: dict | None = None,
    incremental_sync_config: dict | None = None,
    fingerprint_only: bool = False,
    table_suffix: str | None = None,
    suffix_exclude: list[str] | None = None,
    force_full_sync: bool = False,
    skip_phase2: bool = False,
) -> None:
    """Background task to run the extraction triggered by a user."""
    # Rename so process_query() can shadow table_suffix per query
    _job_table_suffix = table_suffix
    try:
        import polars as pl

        settings = get_settings()
        version_tags_tbl = settings.version_tags_table

        # --- Two-phase sync setup ---
        _two_phase = settings.two_phase_sync
        _fp_cache: FingerprintCache | None = None
        _sync_queue: SyncQueue | None = None
        if _two_phase:
            _fp_cache = FingerprintCache()
            _sync_queue = SyncQueue()
            # Cold-start: if the local cache has no data for this country,
            # download fingerprints from Databricks once (requires warehouse).
            # This is critical even when skip_phase2 is set — without cached
            # fingerprints, diff-sync treats every table as first-sync and
            # enqueues a destructive CTAS that replaces the entire table with
            # just the changed periods' data.
            if _fp_cache.is_empty(job.country, "", "period"):
                logger.info(
                    f"Two-phase: cold-start fingerprint cache for {job.country}"
                )
                try:
                    _fp_cache.sync_from_databricks(
                        client, settings.fingerprint_table, country=job.country,
                    )
                except Exception as e:
                    # Cold-start failed — fall back to single-phase for this
                    # job to avoid treating every table as first-sync.
                    logger.warning(
                        f"Two-phase: cache cold-start failed, falling back "
                        f"to single-phase for this job: {e}"
                    )
                    _two_phase = False
                    _fp_cache = None
                    _sync_queue = None
            if _two_phase:
                logger.info(f"Two-phase sync enabled for job {job.job_id}")

        job.status = JobStatus.RUNNING
        job.started_at = datetime.utcnow()
        if not skip_phase2:
            update_job_status(client, table, job.job_id, "running", started_at=job.started_at)
        if db_path:
            local_store.update_job(db_path, job.job_id, status="running", started_at=job.started_at.isoformat())

        results_lock = threading.Lock()
        # Get the in-flight record so we can update its running_queries for live API reads
        record = _trigger_jobs.get(job.job_id)

        def _persist_progress() -> None:
            """Write current progress to SQLite under lock (already held)."""
            if not db_path:
                return
            try:
                running_set = record.running_queries if record else set()
                serialized_results = json.dumps(
                    [r.model_dump(mode="json") for r in job.results]
                )
                failed_queries = json.dumps(
                    [r.query_name for r in job.results if r.status == JobStatus.FAILED]
                )
                local_store.update_job(
                    db_path,
                    job.job_id,
                    current_query=next(iter(running_set), None),
                    running_queries=json.dumps(sorted(running_set)),
                    results=serialized_results,
                    failed_queries=failed_queries,
                )
            except Exception as persist_err:
                logger.warning(f"Failed to persist progress for {job.job_id}: {persist_err}")

        # Pre-compute the set of tables excluded from suffix
        _suffix_exclude_set = set(suffix_exclude or [])

        def process_query(query_name: str) -> QueryResult:
            """Process a single query — runs inside ThreadPoolExecutor."""
            # Per-query effective suffix: excluded tables get no suffix
            table_suffix = (
                None if query_name in _suffix_exclude_set else _job_table_suffix
            )

            # Skip if job was cancelled
            if job.status == JobStatus.CANCELLED:
                return QueryResult(query_name=query_name, status=JobStatus.CANCELLED)

            result = QueryResult(query_name=query_name, status=JobStatus.RUNNING)
            start_time = datetime.utcnow()
            logger.info(f"[{job.country}] {query_name} ...")

            with results_lock:
                job.results.append(result)
                if record:
                    record.running_queries.add(query_name)
                _persist_progress()

            diff_config = (differential_sync_config or {}).get(query_name)
            incr_config = (incremental_sync_config or {}).get(query_name)

            try:
                if diff_config:
                    # --- DIFFERENTIAL SYNC PATH ---
                    logger.info(f"Using differential sync for {query_name} ({job.country})")
                    lookback = override_lookback_months if override_lookback_months is not None else settings.lookback_months
                    # Build WHERE clause from lookback_months
                    l1_col = diff_config.get("level1_column", "periodo")
                    l2_col = diff_config.get("level2_column", "idproduto")
                    where = diff_config.get("where_clause", "")

                    # Build base query from the query loader (supports computed columns like periodo)
                    from dateutil.relativedelta import relativedelta
                    _diff_now = datetime.utcnow()
                    _diff_start = _diff_now - relativedelta(months=lookback or 24)
                    diff_base_query = extractor.query_loader.get_query(query_name, job.country)
                    diff_base_query = diff_base_query.replace("{lookback_months}", str(lookback or 24))
                    diff_base_query = diff_base_query.replace("{start_period}", _diff_start.strftime("%Y%m"))
                    diff_base_query = diff_base_query.replace("{end_period}", _diff_now.strftime("%Y%m"))
                    diff_base_query = diff_base_query.replace("{start_year}", str(_diff_start.year))
                    diff_base_query = diff_base_query.replace("{end_year}", str(_diff_now.year))
                    diff_base_query = diff_base_query.replace("{start_date}", f"'{_diff_start.strftime('%Y-%m-01')}'")
                    diff_base_query = diff_base_query.replace("{end_date}", f"'{_diff_now.strftime('%Y-%m-%d')}'")

                    # When using base_query, the query's own WHERE already scopes the data
                    # so no additional where_clause is needed for fingerprints
                    where = ""

                    result.table_name = writer.resolve_table_name(
                        query_name, job.country, table_suffix=table_suffix,
                    )

                    def _diff_rows_progress(chunk_rows, total_so_far, estimated_total):
                        result.rows_downloaded = total_so_far
                        if estimated_total is not None:
                            result.estimated_rows = estimated_total
                        with results_lock:
                            _persist_progress()

                    diff_stats = run_differential_sync(
                        sql_client=extractor.sql_client,
                        dbx_client=client,
                        writer=writer,
                        sql_table=query_name,
                        country=job.country,
                        level1_column=l1_col,
                        level2_column=l2_col,
                        fingerprint_table=settings.fingerprint_table,
                        where_clause=where,
                        tag=tag,
                        job_id=job.job_id,
                        chunk_size=job.chunk_size,
                        on_rows_progress=_diff_rows_progress,
                        base_query=diff_base_query,
                        mutable_months=diff_config.get("mutable_months", 0),
                        fingerprint_only=fingerprint_only,
                        checksum_columns=diff_config.get("checksum_columns"),
                        table_suffix=table_suffix,
                        two_phase=_two_phase,
                        fingerprint_cache=_fp_cache,
                        sync_queue=_sync_queue,
                    )

                    result.rows_extracted = diff_stats.rows_downloaded
                    result.table_name = writer.resolve_table_name(
                        query_name, job.country, table_suffix=table_suffix,
                    )

                    # Tag the new version (skip when Phase 2 is deferred —
                    # table hasn't been written yet)
                    if tag and result.table_name and diff_stats.rows_downloaded > 0 and not skip_phase2:
                        try:
                            version = writer.get_current_version(result.table_name)
                            insert_version_tag(
                                client, version_tags_tbl,
                                table_name=result.table_name, version=version,
                                tag=tag, created_by=triggered_by or "system",
                                job_id=job.job_id,
                            )
                        except Exception as tag_err:
                            logger.warning(f"Failed to tag {result.table_name}: {tag_err}")
                elif incr_config:
                    # --- INCREMENTAL SYNC PATH (append-only / hybrid tables) ---
                    key_col = incr_config["key_column"]
                    date_col = incr_config.get("date_column")
                    mutable_months = incr_config.get("mutable_window_months", 0)
                    table_name = writer.resolve_table_name(
                        query_name, job.country, table_suffix=table_suffix,
                    )
                    max_key = writer.get_max_key(table_name, key_col)

                    lookback = override_lookback_months if override_lookback_months is not None else settings.lookback_months
                    from dateutil.relativedelta import relativedelta as _rd
                    _now = datetime.utcnow()

                    # Helper: build base SQL with lookback substitutions
                    def _build_base_query() -> str:
                        q = extractor.query_loader.get_query(query_name, job.country)
                        _start = _now - _rd(months=lookback or 24)
                        q = q.replace("{lookback_months}", str(lookback or 24))
                        q = q.replace("{start_period}", _start.strftime("%Y%m"))
                        q = q.replace("{end_period}", _now.strftime("%Y%m"))
                        q = q.replace("{start_year}", str(_start.year))
                        q = q.replace("{end_year}", str(_now.year))
                        q = q.replace("{start_date}", f"'{_start.strftime('%Y-%m-01')}'")
                        q = q.replace("{end_date}", f"'{_now.strftime('%Y-%m-%d')}'")
                        return q

                    # Helper: extract + write
                    def _extract_and_write(sql: str, append: bool, label: str) -> int:
                        chunk_dir = Path(".bridge_data") / "chunks" / job.job_id / f"{query_name}_{label}"

                        def _on_progress(chunk_rows, rows_so_far):
                            result.rows_downloaded += chunk_rows
                            with results_lock:
                                _persist_progress()

                        def _is_cancelled():
                            return job.status == JobStatus.CANCELLED

                        parts, total = extractor.sql_client.execute_query_to_disk(
                            sql, chunk_dir, chunk_size=job.chunk_size,
                            on_progress=_on_progress, is_cancelled=_is_cancelled,
                        )
                        if job.status == JobStatus.CANCELLED:
                            raise InterruptedError("Cancelled")
                        if parts and total > 0:
                            import polars as _pl
                            import glob as _glob
                            _files = sorted(_glob.glob(str(chunk_dir / "*.parquet")))
                            combined = _pl.concat([_pl.read_parquet(f) for f in _files], how="diagonal_relaxed")
                            if append:
                                writer.append_dataframe(
                                    combined, query_name, job.country,
                                    tag=tag, table_suffix=table_suffix,
                                )
                            else:
                                writer.write_dataframe(
                                    combined, query_name, job.country,
                                    tag=tag, table_suffix=table_suffix,
                                )
                        try:
                            if chunk_dir.exists():
                                shutil.rmtree(chunk_dir)
                        except Exception:
                            pass
                        return total

                    result.table_name = table_name
                    total_extracted = 0

                    if max_key is None:
                        # --- First sync: full download ---
                        logger.info(f"Incremental {query_name}: first sync (full download)")
                        base_sql = _build_base_query()
                        try:
                            count_df = extractor.sql_client.execute_query(
                                f"SELECT COUNT(*) AS cnt FROM ({base_sql}) AS _c"
                            )
                            result.estimated_rows = int(count_df.item(0, 0))
                            logger.info(f"Incremental {query_name}: {result.estimated_rows:,} rows")
                        except Exception:
                            pass
                        with results_lock:
                            _persist_progress()
                        total_extracted = _extract_and_write(base_sql, append=False, label="full")
                    else:
                        # --- Subsequent sync ---
                        # PART 1: Mutable window (last N months) — checksum then replace if changed
                        window_extracted = 0
                        if mutable_months and date_col:
                            # Use DATEADD directly (avoids locale/format issues)
                            window_filter = f"DATEADD(MONTH, -{mutable_months}, GETDATE())"
                            # Checksum the mutable window on SQL Server
                            base_sql = _build_base_query()
                            cksum_sql = (
                                f"SELECT COUNT(*) AS cnt, CHECKSUM_AGG(CHECKSUM(*)) AS cksum "
                                f"FROM ({base_sql}) AS _base "
                                f"WHERE [{date_col}] >= {window_filter}"
                            )
                            try:
                                cksum_df = extractor.sql_client.execute_query(cksum_sql)
                                sql_count = int(cksum_df.item(0, 0))
                                sql_cksum = int(cksum_df.item(0, 1)) if cksum_df.item(0, 1) is not None else 0
                            except Exception as e:
                                logger.warning(f"Checksum query failed for {query_name}: {e}")
                                sql_count, sql_cksum = -1, -1

                            # Load stored checksum from fingerprint table
                            from sql_databricks_bridge.core.fingerprint import load_stored_fingerprints, save_fingerprints, Fingerprint
                            stored = load_stored_fingerprints(
                                client, settings.fingerprint_table,
                                job.country, query_name, level="window",
                            )
                            stored_count = int(stored[0].row_count) if stored else -1
                            stored_cksum = int(stored[0].checksum_xor) if stored else -1

                            if sql_count == stored_count and sql_cksum == stored_cksum:
                                logger.info(
                                    f"Incremental {query_name}: mutable window ({mutable_months}mo) "
                                    f"unchanged (count={sql_count}, cksum={sql_cksum}), skipping"
                                )
                            else:
                                logger.info(
                                    f"Incremental {query_name}: mutable window ({mutable_months}mo) "
                                    f"changed (count {stored_count}->{sql_count}, cksum {stored_cksum}->{sql_cksum}), "
                                    f"re-downloading"
                                )
                                result.estimated_rows = sql_count
                                with results_lock:
                                    _persist_progress()
                                # Extract the mutable window
                                window_sql = (
                                    f"SELECT * FROM ({base_sql}) AS _base "
                                    f"WHERE [{date_col}] >= {window_filter}"
                                )
                                # Delete the window from Databricks, then insert fresh data
                                try:
                                    delete_sql = (
                                        f"DELETE FROM {table_name} "
                                        f"WHERE `{date_col}` >= date_sub(current_date(), {mutable_months * 30})"
                                    )
                                    client.execute_sql(delete_sql)
                                    logger.info(f"Deleted mutable window from {table_name} ({mutable_months} months)")
                                except Exception as del_err:
                                    logger.warning(f"Failed to delete mutable window: {del_err}")

                                window_extracted = _extract_and_write(window_sql, append=True, label="window")
                                logger.info(f"Incremental {query_name}: replaced {window_extracted:,} rows in mutable window")

                            # Save current checksum
                            if sql_count >= 0:
                                save_fingerprints(
                                    client, settings.fingerprint_table,
                                    job.country, query_name, level="window",
                                    fingerprints=[Fingerprint(value="mutable_window", row_count=sql_count, checksum_xor=sql_cksum)],
                                    job_id=job.job_id,
                                )

                        # PART 2: New rows beyond watermark — batched download with
                        # advancing watermark.  Each batch is an independent SQL query
                        # ordered by `key_col`, so a mid-stream TCP failure only loses
                        # the current batch and the loop resumes from the last written
                        # row (no wasted progress).
                        base_sql = _build_base_query()

                        def _quote_key(v):
                            if isinstance(v, (int, float)):
                                return str(v)
                            s = str(v)
                            # SQL Server DATETIME only supports ms precision —
                            # truncate microsecond/nanosecond fractions so the
                            # predicate literal parses.
                            if "." in s:
                                base, frac = s.rsplit(".", 1)
                                frac_digits = "".join(c for c in frac if c.isdigit())
                                if frac_digits and len(frac_digits) > 3:
                                    s = base + "." + frac_digits[:3]
                            return "'" + s.replace("'", "''") + "'"

                        extra_filter = ""
                        if mutable_months and date_col:
                            extra_filter = (
                                f" AND [{date_col}] < "
                                f"DATEADD(MONTH, -{mutable_months}, GETDATE())"
                            )

                        count_sql = (
                            f"SELECT COUNT(*) AS cnt FROM ({base_sql}) AS _incr "
                            f"WHERE [{key_col}] > {_quote_key(max_key)}{extra_filter}"
                        )
                        try:
                            count_df = extractor.sql_client.execute_query(count_sql)
                            new_count = int(count_df.item(0, 0))
                        except Exception:
                            new_count = -1

                        if new_count == 0:
                            logger.info(
                                f"Incremental {query_name}: no new rows beyond watermark "
                                f"({key_col} > {max_key})"
                            )
                        elif new_count != 0:
                            if new_count > 0:
                                result.estimated_rows = (result.estimated_rows or 0) + new_count
                                with results_lock:
                                    _persist_progress()
                                logger.info(
                                    f"Incremental {query_name}: {new_count:,} new rows "
                                    f"({key_col} > {max_key})"
                                )

                            batch_size_cfg = int(incr_config.get("batch_size") or 50000)

                            # Tiebreaker column — ensures correctness when
                            # `key_col` has ties (bulk inserts share a
                            # timestamp).  TOP N with `ORDER BY key` alone can
                            # cut a tied group mid-way, then `WHERE key > max`
                            # skips the remainder.  With a unique tiebreaker we
                            # resume via `(key,tie) > (max_key,max_tie)` and
                            # never drop rows.  Auto-detect `entryID`/`id`.
                            def _find_col(df_cols, candidates):
                                lower_map = {c.lower(): c for c in df_cols}
                                for cand in candidates:
                                    if cand.lower() in lower_map:
                                        return lower_map[cand.lower()]
                                return None

                            def _quote_tie(v):
                                if isinstance(v, (int, float)):
                                    return str(v)
                                return "'" + str(v).replace("'", "''") + "'"

                            def _extract_batch(sql: str, label: str):
                                chunk_dir = (
                                    Path(".bridge_data") / "chunks"
                                    / job.job_id / f"{query_name}_{label}"
                                )

                                def _on_progress(chunk_rows, rows_so_far):
                                    result.rows_downloaded += chunk_rows
                                    with results_lock:
                                        _persist_progress()

                                def _is_cancelled():
                                    return job.status == JobStatus.CANCELLED

                                parts, total = extractor.sql_client.execute_query_to_disk(
                                    sql, chunk_dir, chunk_size=job.chunk_size,
                                    on_progress=_on_progress, is_cancelled=_is_cancelled,
                                )
                                if job.status == JobStatus.CANCELLED:
                                    raise InterruptedError("Cancelled")

                                new_max = None
                                new_tie = None
                                tie_col_out = None
                                if parts and total > 0:
                                    import polars as _pl
                                    import glob as _glob
                                    _files = sorted(_glob.glob(str(chunk_dir / "*.parquet")))
                                    combined = _pl.concat(
                                        [_pl.read_parquet(f) for f in _files],
                                        how="diagonal_relaxed",
                                    )
                                    actual_key = _find_col(
                                        combined.columns, [key_col],
                                    )
                                    actual_tie = _find_col(
                                        combined.columns,
                                        ["entryID", "entry_id", "id"],
                                    )
                                    if actual_tie == actual_key:
                                        actual_tie = None

                                    if actual_key is not None:
                                        raw = combined[actual_key].max()
                                        # SQL Server DATETIME only supports 3
                                        # decimal places — truncate to ms so the
                                        # next batch's literal parses.
                                        if hasattr(raw, "strftime"):
                                            new_max = (
                                                raw.strftime("%Y-%m-%d %H:%M:%S.")
                                                + f"{raw.microsecond // 1000:03d}"
                                            )
                                        else:
                                            s = str(raw)
                                            if "." in s and len(s.rsplit(".", 1)[-1]) > 3:
                                                new_max = s[: s.rfind(".") + 4]
                                            else:
                                                new_max = s

                                        if actual_tie is not None:
                                            tie_rows = combined.filter(
                                                _pl.col(actual_key) == raw
                                            )
                                            if len(tie_rows) > 0:
                                                new_tie = tie_rows[actual_tie].max()
                                                tie_col_out = actual_tie
                                    writer.append_dataframe(
                                        combined, query_name, job.country,
                                        tag=tag, table_suffix=table_suffix,
                                    )
                                try:
                                    if chunk_dir.exists():
                                        shutil.rmtree(chunk_dir)
                                except Exception:
                                    pass
                                return total, new_max, new_tie, tie_col_out

                            # Preflight: detect tiebreaker column from the
                            # source schema so batch 1 can already use compound
                            # ORDER BY.  Without this, SQL Server picks an
                            # arbitrary subset within a tied group at batch's
                            # cutoff, and we may skip rows on the next batch.
                            tie_col = None
                            try:
                                _schema_df = extractor.sql_client.execute_query(
                                    f"SELECT TOP 0 * FROM ({base_sql}) AS _schema"
                                )
                                tie_col = _find_col(
                                    list(_schema_df.columns),
                                    ["entryID", "entry_id", "id"],
                                )
                                if tie_col and tie_col.lower() == key_col.lower():
                                    tie_col = None
                            except Exception as e:
                                logger.warning(
                                    f"Incremental {query_name}: tiebreaker schema probe "
                                    f"failed ({e}); falling back to single-column ORDER BY"
                                )

                            current_key = max_key
                            current_tie = None
                            batch_num = 0
                            batched_total = 0
                            while True:
                                batch_num += 1
                                # Build compound-key predicate when a tiebreaker
                                # is known from a prior batch; otherwise fall
                                # back to a single-column predicate.
                                if tie_col is not None and current_tie is not None:
                                    key_filter = (
                                        f"(([{key_col}] > {_quote_key(current_key)}) "
                                        f"OR ([{key_col}] = {_quote_key(current_key)} "
                                        f"AND [{tie_col}] > {_quote_tie(current_tie)}))"
                                    )
                                else:
                                    key_filter = (
                                        f"[{key_col}] > {_quote_key(current_key)}"
                                    )
                                # Always compound ORDER BY when tiebreaker is
                                # known upfront — prevents SQL Server from
                                # picking an arbitrary subset at the batch
                                # cutoff within a tied group.
                                order_by = (
                                    f"[{key_col}], [{tie_col}]"
                                    if tie_col is not None
                                    else f"[{key_col}]"
                                )

                                batch_sql = (
                                    f"SELECT TOP {batch_size_cfg} * "
                                    f"FROM ({base_sql}) AS _incr "
                                    f"WHERE {key_filter}{extra_filter} "
                                    f"ORDER BY {order_by}"
                                )
                                (
                                    batch_count,
                                    batch_max,
                                    batch_tie,
                                    batch_tie_col,
                                ) = _extract_batch(batch_sql, f"new_b{batch_num}")
                                batched_total += batch_count
                                logger.info(
                                    f"Incremental {query_name}: batch {batch_num} "
                                    f"committed {batch_count:,} rows "
                                    f"(total {batched_total:,}"
                                    + (f"/{new_count:,}" if new_count > 0 else "")
                                    + f", new max={batch_max}"
                                    + (f"/{batch_tie_col}={batch_tie}" if batch_tie_col else "")
                                    + ")"
                                )
                                if batch_count < batch_size_cfg or batch_max is None:
                                    break
                                current_key = batch_max
                                current_tie = batch_tie
                                # tie_col is set upfront via schema probe; do
                                # NOT overwrite here.

                            window_extracted += batched_total

                        total_extracted = window_extracted

                    result.rows_extracted = total_extracted
                    if total_extracted > 0:
                        logger.info(f"Incremental {query_name}: total {total_extracted:,} rows synced")
                        if tag and result.table_name:
                            try:
                                version = writer.get_current_version(result.table_name)
                                insert_version_tag(
                                    client, version_tags_tbl,
                                    table_name=result.table_name, version=version,
                                    tag=tag, created_by=triggered_by or "system",
                                    job_id=job.job_id,
                                )
                            except Exception as tag_err:
                                logger.warning(f"Failed to tag {result.table_name}: {tag_err}")
                    else:
                        logger.info(f"Incremental {query_name}: nothing to sync")

                elif fingerprint_only:
                    # --- FINGERPRINT-ONLY PATH (no diff_config, no incr_config) ---
                    logger.info(f"Fingerprint-only mode for {query_name} ({job.country})")
                    lookback = override_lookback_months if override_lookback_months is not None else settings.lookback_months
                    from dateutil.relativedelta import relativedelta as _fp_rd
                    _fp_now = datetime.utcnow()
                    _fp_start = _fp_now - _fp_rd(months=lookback or 24)
                    fp_base_query = extractor.query_loader.get_query(query_name, job.country)
                    fp_base_query = fp_base_query.replace("{lookback_months}", str(lookback or 24))
                    fp_base_query = fp_base_query.replace("{start_period}", _fp_start.strftime("%Y%m"))
                    fp_base_query = fp_base_query.replace("{end_period}", _fp_now.strftime("%Y%m"))
                    fp_base_query = fp_base_query.replace("{start_year}", str(_fp_start.year))
                    fp_base_query = fp_base_query.replace("{end_year}", str(_fp_now.year))
                    fp_base_query = fp_base_query.replace("{start_date}", f"'{_fp_start.strftime('%Y-%m-01')}'")
                    fp_base_query = fp_base_query.replace("{end_date}", f"'{_fp_now.strftime('%Y-%m-%d')}'")

                    from sql_databricks_bridge.core.fingerprint import (
                        compute_level1_fingerprints as _compute_l1,
                        save_fingerprints as _save_fps,
                        ensure_fingerprint_table as _ensure_fp_table,
                    )
                    _ensure_fp_table(client, settings.fingerprint_table)
                    fp_l1 = _compute_l1(
                        extractor.sql_client, query_name, "periodo",
                        base_query=fp_base_query,
                    )
                    _save_fps(
                        client, settings.fingerprint_table,
                        job.country, query_name, level="period",
                        fingerprints=fp_l1, job_id=job.job_id,
                    )
                    result.rows_extracted = 0
                    logger.info(
                        f"Fingerprint-only {query_name}: saved {len(fp_l1)} L1 fingerprints"
                    )

                else:
                    # --- FULL EXTRACTION PATH (disk streaming + retry) ---
                    row_limit = override_row_limit if override_row_limit is not None else (settings.query_row_limit or None)
                    lookback = override_lookback_months if override_lookback_months is not None else settings.lookback_months
                    max_retries = 3
                    base_delay = 30

                    # Build the SQL query via the extractor's query loader
                    query_sql = extractor.query_loader.get_query(query_name, job.country)
                    query_sql = query_sql.replace("{lookback_months}", str(lookback or 24))
                    from dateutil.relativedelta import relativedelta as _rd
                    _now = datetime.utcnow()
                    _start = _now - _rd(months=lookback or 24)
                    query_sql = query_sql.replace("{start_period}", _start.strftime("%Y%m"))
                    query_sql = query_sql.replace("{end_period}", _now.strftime("%Y%m"))
                    query_sql = query_sql.replace("{start_year}", str(_start.year))
                    query_sql = query_sql.replace("{end_year}", str(_now.year))
                    query_sql = query_sql.replace("{start_date}", f"'{_start.strftime('%Y-%m-01')}'")
                    query_sql = query_sql.replace("{end_date}", f"'{_now.strftime('%Y-%m-%d')}'")
                    if row_limit is not None and row_limit > 0:
                        query_sql = f"SELECT TOP {row_limit} * FROM ({query_sql}) AS _limited_subquery"

                    # --- Skip-if-unchanged check for dimension tables ---
                    # Compute COUNT + CHECKSUM_AGG on SQL Server and compare to stored
                    # fingerprint.  If identical, skip the extraction entirely.
                    _skip_unchanged = False
                    if not force_full_sync:
                        try:
                            _dim_cksum_sql = (
                                f"SELECT COUNT(*) AS cnt, CHECKSUM_AGG(CHECKSUM(*)) AS cksum "
                                f"FROM ({query_sql}) AS _dim_ck"
                            )
                            _dim_ck_df = extractor.sql_client.execute_query(
                                _dim_cksum_sql,
                                timeout_seconds=settings.sql_server.dimension_query_timeout_seconds,
                            )
                            _dim_count = int(_dim_ck_df.item(0, 0))
                            _dim_cksum = int(_dim_ck_df.item(0, 1)) if _dim_ck_df.item(0, 1) is not None else 0

                            # Effective fingerprint name (suffix-aware)
                            _dim_fp_name = f"{query_name}{table_suffix}" if table_suffix else query_name

                            from sql_databricks_bridge.core.fingerprint import (
                                load_stored_fingerprints as _dim_load_fp,
                                save_fingerprints as _dim_save_fp,
                                Fingerprint as _DimFP,
                            )

                            # Try SQLite cache first, then Databricks fallback.
                            _dim_stored = []
                            if _fp_cache is not None:
                                _dim_stored = _fp_cache.load(job.country, _dim_fp_name, "dimension")
                            if not _dim_stored:
                                _dim_stored = _dim_load_fp(
                                    client, settings.fingerprint_table,
                                    job.country, _dim_fp_name, level="dimension",
                                )

                            if _dim_stored:
                                _stored_cnt = int(_dim_stored[0].row_count)
                                _stored_ck = int(_dim_stored[0].checksum_xor)
                                if _dim_count == _stored_cnt and _dim_cksum == _stored_ck:
                                    _skip_unchanged = True
                                    logger.info(
                                        f"Dimension skip {query_name} ({job.country}): "
                                        f"unchanged (count={_dim_count}, cksum={_dim_cksum})"
                                    )
                                else:
                                    logger.info(
                                        f"Dimension changed {query_name} ({job.country}): "
                                        f"count {_stored_cnt}->{_dim_count}, "
                                        f"cksum {_stored_ck}->{_dim_cksum}"
                                    )
                        except Exception as _dim_err:
                            logger.debug(f"Dimension checksum failed for {query_name}: {_dim_err}")

                    if _skip_unchanged:
                        result.rows_extracted = 0
                        result.status = JobStatus.COMPLETED
                        result.table_name = writer.resolve_table_name(
                            query_name, job.country, table_suffix=table_suffix,
                        )
                        result.duration_seconds = (
                            datetime.utcnow() - start_time
                        ).total_seconds()
                        with results_lock:
                            if record:
                                record.running_queries.discard(query_name)
                            _persist_progress()
                        return result

                    # Pre-flight COUNT for progress tracking
                    try:
                        count_sql = f"SELECT COUNT(*) AS cnt FROM ({query_sql}) AS _cnt_sub"
                        count_df = extractor.sql_client.execute_query(count_sql)
                        result.estimated_rows = int(count_df.item(0, 0))
                        logger.info(f"Query {query_name}: estimated {result.estimated_rows:,} rows")
                    except Exception as count_err:
                        logger.warning(f"COUNT(*) failed for {query_name}: {count_err}")
                        result.estimated_rows = 0

                    with results_lock:
                        _persist_progress()

                    # Disk-streaming extraction with retry
                    chunk_dir = Path(".bridge_data") / "chunks" / job.job_id / query_name
                    parts = None
                    total_rows = 0

                    def _on_chunk_progress(chunk_rows: int, rows_so_far: int) -> None:
                        result.rows_downloaded = rows_so_far
                        with results_lock:
                            _persist_progress()

                    def _is_cancelled() -> bool:
                        return job.status == JobStatus.CANCELLED

                    for attempt in range(max_retries + 1):
                        try:
                            # Clean up partial data from previous attempts
                            if chunk_dir.exists():
                                shutil.rmtree(chunk_dir)
                            parts, total_rows = extractor.sql_client.execute_query_to_disk(
                                query_sql, chunk_dir, chunk_size=job.chunk_size,
                                on_progress=_on_chunk_progress,
                                is_cancelled=_is_cancelled,
                                timeout_seconds=settings.sql_server.dimension_query_timeout_seconds,
                            )
                            break
                        except Exception as retry_err:
                            err_str = str(retry_err)
                            from sql_databricks_bridge.core.transient_errors import (
                                is_transient as _is_transient,
                                is_lock_timeout as _is_lock_timeout,
                                backoff_seconds as _backoff_seconds,
                            )
                            if _is_transient(err_str) and attempt < max_retries:
                                delay = _backoff_seconds(err_str, attempt, base=base_delay)
                                kind = "lock-contention" if _is_lock_timeout(err_str) else "transient"
                                logger.warning(
                                    f"{kind.capitalize()} SQL error on {query_name} "
                                    f"(attempt {attempt+1}/{max_retries+1}), retrying in {delay}s: "
                                    f"{err_str[:200]}"
                                )
                                time.sleep(delay)
                                continue
                            raise

                    # Check if cancelled during download
                    if job.status == JobStatus.CANCELLED:
                        result.status = JobStatus.CANCELLED
                        result.error = "Cancelled by user"
                        logger.info(f"Query {query_name} cancelled during download")
                        # Clean up partial data
                        try:
                            if chunk_dir.exists():
                                shutil.rmtree(chunk_dir)
                        except Exception:
                            pass
                        return result

                    if parts and total_rows > 0:
                        import polars as _pl
                        import glob as _glob
                        _files = sorted(_glob.glob(str(chunk_dir / "*.parquet")))
                        combined = _pl.concat([_pl.read_parquet(f) for f in _files], how="diagonal_relaxed")

                        if _two_phase and _sync_queue is not None:
                            # Two-phase: upload to Volume and enqueue CTAS
                            combined_lower = combined.rename({c: c.lower() for c in combined.columns})
                            _tp_s = settings.databricks
                            _tp_staging = (
                                f"/Volumes/{_tp_s.catalog}/{job.country}/{_tp_s.volume}"
                                f"/_staging_2p/{query_name}_{job.job_id}"
                            )
                            writer.client.upload_dataframe_chunked(
                                [combined_lower], _tp_staging,
                            )
                            _sync_queue.enqueue(
                                job_id=job.job_id, country=job.country,
                                table_name=query_name, operation="ctas",
                                staging_path=_tp_staging,
                                tag=tag, table_suffix=table_suffix or "",
                            )
                            logger.info(
                                f"Two-phase: enqueued CTAS for {query_name} "
                                f"({total_rows:,} rows staged)"
                            )
                        else:
                            writer.write_dataframe(
                                combined, query_name, job.country,
                                tag=tag, table_suffix=table_suffix,
                            )

                            # Tag the new version with the job tag
                            if tag and result.table_name:
                                try:
                                    version = writer.get_current_version(result.table_name)
                                    insert_version_tag(
                                        client,
                                        version_tags_tbl,
                                        table_name=result.table_name,
                                        version=version,
                                        tag=tag,
                                        created_by=triggered_by or "system",
                                        job_id=job.job_id,
                                    )
                                    logger.info(
                                        f"Tagged {result.table_name} v{version} as '{tag}'"
                                    )
                                except Exception as tag_err:
                                    logger.warning(
                                        f"Failed to tag {result.table_name}: {tag_err}"
                                    )

                        result.rows_extracted = total_rows
                        result.table_name = writer.resolve_table_name(
                            query_name, job.country, table_suffix=table_suffix,
                        )
                    else:
                        result.rows_extracted = 0

                    # Save dimension fingerprint for skip-if-unchanged on next run
                    if result.rows_extracted and result.rows_extracted > 0:
                        try:
                            _dim_fp_name = f"{query_name}{table_suffix}" if table_suffix else query_name
                            _dim_cksum_sql = (
                                f"SELECT COUNT(*) AS cnt, CHECKSUM_AGG(CHECKSUM(*)) AS cksum "
                                f"FROM ({query_sql}) AS _dim_ck"
                            )
                            _dim_ck_df = extractor.sql_client.execute_query(_dim_cksum_sql)
                            _dim_count = int(_dim_ck_df.item(0, 0))
                            _dim_cksum = int(_dim_ck_df.item(0, 1)) if _dim_ck_df.item(0, 1) is not None else 0

                            from sql_databricks_bridge.core.fingerprint import (
                                save_fingerprints as _dim_save_fp,
                                Fingerprint as _DimFP,
                                ensure_fingerprint_table as _dim_ensure_fp,
                            )
                            _dim_fp = [_DimFP(value="__all__", row_count=_dim_count, checksum_xor=_dim_cksum)]

                            if _two_phase and _sync_queue is not None:
                                # Enqueue fingerprint save for Phase 2
                                _sync_queue.enqueue(
                                    job_id=job.job_id,
                                    country=job.country,
                                    table_name=_dim_fp_name,
                                    operation="save_fingerprints",
                                    metadata={
                                        "level": "dimension",
                                        "fingerprints": [
                                            {"value": fp.value, "row_count": fp.row_count, "checksum_xor": fp.checksum_xor}
                                            for fp in _dim_fp
                                        ],
                                    },
                                    tag=tag, table_suffix=table_suffix or "",
                                )
                            else:
                                _dim_ensure_fp(client, settings.fingerprint_table)
                                _dim_save_fp(
                                    client, settings.fingerprint_table,
                                    job.country, _dim_fp_name, level="dimension",
                                    fingerprints=_dim_fp, job_id=job.job_id,
                                )

                            # Save to local cache only in single-phase (immediate commit).
                            # In two-phase, Phase 2's _execute_save_fingerprints handles it.
                            if not _two_phase and _fp_cache is not None:
                                _fp_cache.save(job.country, _dim_fp_name, "dimension", _dim_fp)

                            logger.info(
                                f"Saved dimension fingerprint for {query_name} ({job.country}): "
                                f"count={_dim_count}, cksum={_dim_cksum}"
                            )
                        except Exception as _dim_save_err:
                            logger.warning(f"Failed to save dimension fingerprint for {query_name}: {_dim_save_err}")

                    # Clean up temp chunks
                    try:
                        if chunk_dir.exists():
                            shutil.rmtree(chunk_dir)
                    except Exception:
                        pass

                result.status = JobStatus.COMPLETED

            except InterruptedError:
                result.status = JobStatus.CANCELLED
                result.error = "Cancelled by user"
                logger.info(f"Query {query_name} cancelled")
            except Exception as e:
                result.status = JobStatus.FAILED
                result.error = str(e)
                logger.error(f"Query {query_name} failed: {e}")

            result.duration_seconds = (
                datetime.utcnow() - start_time
            ).total_seconds()

            # Verbose per-table completion log
            _dur = f"{result.duration_seconds:.1f}s"
            if result.status == JobStatus.COMPLETED:
                _rows = result.rows_extracted or 0
                logger.info(f"[{job.country}] {query_name} ... ok ({_rows:,} rows, {_dur})")
            elif result.status == JobStatus.FAILED:
                logger.info(f"[{job.country}] {query_name} ... FAIL ({_dur}) {result.error}")
            elif result.status == JobStatus.CANCELLED:
                logger.info(f"[{job.country}] {query_name} ... cancelled ({_dur})")

            with results_lock:
                if record:
                    record.running_queries.discard(query_name)
                _persist_progress()

            return result

        # Run queries in parallel
        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            futures = {
                executor.submit(process_query, qn): qn
                for qn in job.queries
            }
            for future in as_completed(futures):
                qn = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    logger.error(f"Unexpected error processing query {qn}: {exc}")

        job.current_query = None

        # --- Phase 2: Commit all queued Databricks operations in a tight batch ---
        if _two_phase and _sync_queue is not None and _fp_cache is not None:
            pending_count = _sync_queue.count_pending(job_id=job.job_id)
            if skip_phase2:
                logger.info(
                    f"Phase 2 DEFERRED: {pending_count} operation(s) queued for "
                    f"job {job.job_id} — will be committed via /api/v1/phase2/execute"
                )
            elif pending_count > 0:
                logger.info(
                    f"Phase 2: committing {pending_count} queued operation(s) "
                    f"for job {job.job_id}"
                )
                try:
                    p2_executor = Phase2Executor(
                        dbx_client=client,
                        writer=writer,
                        queue=_sync_queue,
                        fingerprint_cache=_fp_cache,
                        fingerprint_table=settings.fingerprint_table,
                    )
                    p2_result = p2_executor.execute_batch(
                        job_id=job.job_id,
                        max_parallel=settings.max_parallel_queries,
                    )
                    logger.info(
                        f"Phase 2 complete: {p2_result.tables_committed} tables, "
                        f"{p2_result.fingerprints_saved} fp saves, "
                        f"{p2_result.tables_failed} failures in "
                        f"{p2_result.duration_seconds:.1f}s"
                    )
                    if p2_result.errors:
                        for err in p2_result.errors:
                            logger.error(f"Phase 2 error: {err}")
                except Exception as p2_err:
                    logger.error(f"Phase 2 execution failed: {p2_err}")
            else:
                logger.info("Phase 2: no pending operations to commit")
            # Cleanup old queue entries
            try:
                _sync_queue.cleanup_old(days=7)
            except Exception:
                pass

        # Determine if this is a calibration job (should advance to further steps)
        is_calibration = record and record.stage == "calibracion"

        # Don't override if already cancelled by user
        if job.status != JobStatus.CANCELLED:
            all_sync_failed = job.queries_failed > 0 and job.queries_completed == 0
            if all_sync_failed:
                job.status = JobStatus.FAILED
                job.completed_at = job.completed_at or datetime.utcnow()
            elif is_calibration:
                # Calibration steps remain: keep status as RUNNING so the
                # frontend continues polling until the monitor finalizes.
                job.status = JobStatus.RUNNING
                # Don't set completed_at yet -- _finalize_job handles that.
            else:
                # Sync-only job (sincronización page): complete after extraction
                job.status = JobStatus.COMPLETED
                job.completed_at = datetime.utcnow()

        # Persist extraction results
        persist_status = job.status.value
        if not skip_phase2:
            update_job_status(
                client,
                table,
                job.job_id,
                persist_status,
                completed_at=job.completed_at,
            )
        if db_path:
            serialized_results = json.dumps(
                [r.model_dump(mode="json") for r in job.results]
            )
            local_store.update_job(
                db_path,
                job.job_id,
                status=persist_status,
                completed_at=job.completed_at.isoformat() if job.completed_at else None,
                current_query=None,
                running_queries="[]",
                results=serialized_results,
            )

        # Skip calibration advancement if cancelled
        if job.status == JobStatus.CANCELLED:
            if db_path:
                _persist_steps(job.job_id, db_path)
        # Skip calibration advancement for sync-only jobs
        elif not is_calibration:
            calibration_tracker.complete_step(job.job_id, "sync_data")
            if db_path:
                _persist_steps(job.job_id, db_path)
            logger.info("Sync-only job %s completed (stage=%s, not advancing to calibration)",
                        job.job_id, record.stage if record else "?")
        # Update calibration sync_data step
        elif job.queries_failed > 0 and job.queries_completed == 0:
            calibration_tracker.complete_step(
                job.job_id,
                "sync_data",
                error=f"{job.queries_failed} query(ies) failed during sync",
            )
            if db_path:
                _persist_steps(job.job_id, db_path)
        else:
            calibration_tracker.complete_step(job.job_id, "sync_data")
            # Auto-advance to copy_to_calibration
            next_step = calibration_tracker.advance_after_sync(job.job_id)
            if db_path:
                _persist_steps(job.job_id, db_path)
            # Launch the Databricks job for copy_to_calibration
            if next_step:
                period = record.period if record else None
                cal_info = calibration_tracker.get(job.job_id)
                aggregations = cal_info.aggregations if cal_info else None
                _launch_calibration_step(
                    job.job_id, next_step, job.country,
                    period=period, aggregations=aggregations,
                )

    except Exception as e:
        job.current_query = None
        job.status = JobStatus.FAILED
        job.error = str(e)
        logger.error(f"Trigger job {job.job_id} failed: {e}")
        if not skip_phase2:
            update_job_status(client, table, job.job_id, "failed", error=str(e))
        if db_path:
            local_store.update_job(db_path, job.job_id, status="failed", error=str(e), running_queries="[]", current_query=None)
        # Mark sync_data as failed
        calibration_tracker.complete_step(job.job_id, "sync_data", error=str(e))
        if db_path:
            _persist_steps(job.job_id, db_path)


# --- Endpoints ---


@router.post(
    "/trigger",
    response_model=TriggerResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Trigger extraction",
    description="Trigger a data extraction (SQL Server -> Databricks) for a country.",
)
async def trigger_extraction(
    request: TriggerRequest,
    user: CurrentAzureADUser,
    background_tasks: BackgroundTasks,
    raw_request: Request,
) -> TriggerResponse:
    """Trigger extraction for the given country and queries."""
    # Check user has permission for this country
    if not user.can_trigger_sync:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "forbidden",
                "message": "User role does not allow triggering syncs",
            },
        )

    if not user.can_access_country(request.country):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "forbidden",
                "message": f"User not authorized for country '{request.country}'",
            },
        )

    disabled = {
        e.strip() for e in (get_settings().disabled_entries or "").split(",") if e.strip()
    }
    if request.country in disabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "entry_disabled",
                "message": f"Entry '{request.country}' is currently disabled via DISABLED_ENTRIES",
            },
        )

    # Per-entry query blacklist: parse DISABLED_QUERIES into a set for this entry.
    _dq_map: dict[str, set[str]] = {}
    for pair in (get_settings().disabled_queries or "").split(","):
        pair = pair.strip()
        if ":" not in pair:
            continue
        ent, q = pair.split(":", 1)
        _dq_map.setdefault(ent.strip(), set()).add(q.strip())
    disabled_q_for_entry: set[str] = _dq_map.get(request.country, set())

    if disabled_q_for_entry and request.queries:
        overlap = [q for q in request.queries if q in disabled_q_for_entry]
        if overlap:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "query_disabled",
                    "message": f"Queries disabled for '{request.country}': {overlap}",
                },
            )

    # Capture SQLite path for the module (used by finalize_trigger_job)
    global _sqlite_db_path
    if _sqlite_db_path is None:
        _sqlite_db_path = _get_db_path(raw_request)

    # Skip sync: per-request flag OR global server setting
    skip_sync = request.skip_sync or get_settings().skip_sync_data

    if skip_sync:
        job_id = str(_uuid.uuid4())
        tag = build_tag(request.country, request.stage)
        now = datetime.utcnow()

        # Get Databricks client
        db_client = _get_databricks_client(raw_request)
        settings = get_settings()
        jobs_tbl = settings.jobs_table

        # Use a minimal query list for the job record
        queries = request.queries or ["skip_sync_placeholder"]

        # Persist to Databricks Delta table (best-effort, may fail if table missing)
        aggregations_dict = request.aggregations.model_dump() if request.aggregations else None
        try:
            insert_job(
                db_client,
                jobs_tbl,
                job_id=job_id,
                country=request.country,
                stage=request.stage,
                tag=tag,
                queries=queries,
                triggered_by=user.email,
                created_at=now,
                period=request.period,
                aggregations=aggregations_dict,
            )
        except Exception as e:
            logger.warning("SKIP_SYNC_DATA: insert_job to Delta table failed: %s", e)

        # Persist to local SQLite for crash recovery
        db_path = _get_db_path(raw_request)
        if db_path:
            try:
                local_store.insert_job(
                    db_path,
                    job_id=job_id,
                    country=request.country,
                    stage=request.stage,
                    tag=tag,
                    queries=queries,
                    triggered_by=user.email,
                    created_at=now.isoformat(),
                )
            except Exception as sqlite_err:
                logger.warning("SKIP_SYNC_DATA: SQLite insert failed: %s", sqlite_err)

        # Create synthetic ExtractionJob and store in _trigger_jobs so
        # GET /events/{job_id} can serve live progress from memory
        job = ExtractionJob(
            job_id=job_id,
            country=request.country,
            queries=queries,
            status=JobStatus.RUNNING,
            created_at=now,
            started_at=now,
        )
        record = _TriggerJobRecord(
            extractor=None,
            job=job,
            triggered_by=user.email,
            stage=request.stage,
            tag=tag,
            period=request.period,
            aggregations=request.aggregations,
        )
        _trigger_jobs[job_id] = record

        # Persist to SQLite for fast reads
        db_path = _get_db_path(raw_request)
        if db_path:
            try:
                local_store.insert_job(
                    db_path,
                    job_id=job_id,
                    country=request.country,
                    stage=request.stage,
                    tag=tag,
                    queries=queries,
                    triggered_by=user.email,
                    created_at=now.isoformat(),
                    period=request.period,
                )
                # Mark as completed immediately for skip_sync jobs
                local_store.update_job(
                    db_path, job_id,
                    status="completed",
                    started_at=now.isoformat(),
                    completed_at=now.isoformat(),
                )
            except Exception as sqlite_err:
                logger.warning(f"Failed to insert skip_sync job into SQLite: {sqlite_err}")

        # Create calibration tracking - immediately complete sync_data
        agg_dict = request.aggregations.model_dump() if request.aggregations else None
        calibration_tracker.create(
            job_id, country=request.country, period=request.period, aggregations=agg_dict,
        )
        calibration_tracker.start_step(job_id, "sync_data")
        calibration_tracker.complete_step(job_id, "sync_data")

        if request.skip_copy:
            # Also skip copy_to_calibration — jump straight to merge_data
            calibration_tracker.start_step(job_id, "copy_to_calibration")
            calibration_tracker.complete_step(job_id, "copy_to_calibration")
            next_step = "merge_data"
            calibration_tracker.start_step(job_id, next_step)
        else:
            # Advance to copy_to_calibration and launch Databricks job
            next_step = calibration_tracker.advance_after_sync(job_id)

        # Persist steps to SQLite after state transitions
        if db_path:
            _persist_steps(job_id, db_path)
        if next_step:
            _launch_calibration_step(
                job_id, next_step, request.country,
                period=request.period, aggregations=agg_dict,
            )

        logger.info("SKIP_SYNC: job %s created, skipped sync%s, advancing to %s",
                     job_id, "+copy" if request.skip_copy else "", next_step)

        return TriggerResponse(
            job_id=job_id,
            status="pending",
            country=request.country,
            stage=request.stage,
            period=request.period,
            tag=tag,
            queries=queries,
            queries_count=len(queries),
            created_at=now,
            triggered_by=user.email,
        )

    # Create extractor and job
    try:
        queries_path = request.queries_path or get_settings().queries_path
        loader = CountryAwareQueryLoader(queries_path)
        if loader.is_server(request.country):
            server_host = request.country
            if "." not in server_host:
                server_host = f"{server_host}.KT.group.local"
            sql_client = SQLServerClient(server=server_host, database="master")
        else:
            sql_client = SQLServerClient(country=request.country)
        extractor = Extractor(
            queries_path=queries_path,
            sql_client=sql_client,
        )

        effective_queries = request.queries
        if disabled_q_for_entry and effective_queries is None:
            effective_queries = [
                q for q in loader.list_queries(request.country)
                if q not in disabled_q_for_entry
            ]

        job = extractor.create_job(
            country=request.country,
            destination="",
            queries=effective_queries,
        )
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": str(e)},
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "validation_error", "message": str(e)},
        )

    # Compose tag server-side
    tag = build_tag(request.country, request.stage)

    # Get Databricks client and jobs table
    db_client = _get_databricks_client(raw_request)
    settings = get_settings()
    jobs_tbl = settings.jobs_table

    # Persist to Databricks Delta table (best-effort, may fail if unreachable/token expired)
    # When skip_phase2 is set, skip Delta writes to avoid waking the warehouse.
    aggregations_dict = request.aggregations.model_dump() if request.aggregations else None
    if request.skip_phase2:
        logger.info("skip_phase2: skipping insert_job to Delta (SQLite only)")
    else:
        try:
            insert_job(
                db_client,
                jobs_tbl,
                job_id=job.job_id,
                country=request.country,
                stage=request.stage,
                tag=tag,
                queries=job.queries,
                triggered_by=user.email,
                created_at=job.created_at,
                period=request.period,
                aggregations=aggregations_dict,
            )
        except Exception as e:
            logger.warning("insert_job to Delta table failed (job=%s): %s", job.job_id, e)

    # Persist to local SQLite for crash recovery and fast reads
    db_path = _get_db_path(raw_request)
    if db_path:
        try:
            local_store.insert_job(
                db_path,
                job_id=job.job_id,
                country=request.country,
                stage=request.stage,
                tag=tag,
                queries=job.queries,
                triggered_by=user.email,
                created_at=job.created_at.isoformat(),
                period=request.period,
            )
        except Exception as sqlite_err:
            logger.warning(f"Failed to insert job into SQLite: {sqlite_err}")

    # Create calibration step tracking
    agg_dict = request.aggregations.model_dump() if request.aggregations else None
    calibration_tracker.create(
        job.job_id, country=request.country, period=request.period, aggregations=agg_dict,
    )
    calibration_tracker.start_step(job.job_id, "sync_data")
    # Persist initial step state to SQLite
    if db_path:
        _persist_steps(job.job_id, db_path)

    # Store in-flight record (needed for background task reference)
    record = _TriggerJobRecord(
        extractor=extractor,
        job=job,
        triggered_by=user.email,
        stage=request.stage,
        tag=tag,
        period=request.period,
        aggregations=request.aggregations,
    )
    _trigger_jobs[job.job_id] = record

    # Launch background extraction
    writer = DeltaTableWriter(db_client)
    # Build effective diff sync config: merge server-side defaults + client overrides
    diff_sync_cfg = {}
    # 1. Server-side defaults from DIFF_SYNC_TABLES env var
    for entry in (settings.diff_sync_tables or "").split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":")
        tbl = parts[0].strip()
        if tbl:
            cfg = {
                "level1_column": parts[1].strip() if len(parts) > 1 else "periodo",
                "level2_column": parts[2].strip() if len(parts) > 2 else "",
            }
            if len(parts) > 3:
                cfg["mutable_months"] = int(parts[3].strip())
            diff_sync_cfg[tbl] = cfg
    # 1b. Checksum column subsets from DIFF_SYNC_CHECKSUM_COLUMNS env var
    for entry in (settings.diff_sync_checksum_columns or "").split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":")
        tbl = parts[0].strip()
        if tbl and len(parts) > 1 and tbl in diff_sync_cfg:
            cols = [c.strip() for c in parts[1].split("+") if c.strip()]
            if cols:
                diff_sync_cfg[tbl]["checksum_columns"] = cols
    # 2. Client overrides (take precedence)
    if request.differential_sync:
        for k, v in request.differential_sync.items():
            diff_sync_cfg[k] = v.model_dump()
    if not diff_sync_cfg:
        diff_sync_cfg = None
    else:
        # Log which queries will use diff sync (helps debugging)
        diff_tables = list(diff_sync_cfg.keys())
        matching = [q for q in job.queries if q in diff_sync_cfg]
        if matching:
            logger.info("Diff sync enabled for queries: %s (from %s)",
                        matching, "server+client" if request.differential_sync else "server default")

    # Build incremental sync config from INCREMENTAL_SYNC_TABLES env var
    # Format: "table:key_col[:batch_size]" OR "table:key_col:date_col:mutable_months[:batch_size]"
    # batch_size caps each SELECT TOP N (default 50000) — small batches survive
    # TCP timeouts on wide tables (e.g. VARCHAR(MAX) payloads).
    incr_sync_cfg = {}
    for entry in (settings.incremental_sync_tables or "").split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":")
        tbl = parts[0].strip()
        if tbl and len(parts) > 1:
            cfg = {"key_column": parts[1].strip()}
            if len(parts) == 3:
                cfg["batch_size"] = int(parts[2].strip())
            elif len(parts) >= 4:
                cfg["date_column"] = parts[2].strip()
                cfg["mutable_window_months"] = int(parts[3].strip())
                if len(parts) >= 5:
                    cfg["batch_size"] = int(parts[4].strip())
            incr_sync_cfg[tbl] = cfg
    if not incr_sync_cfg:
        incr_sync_cfg = None
    else:
        matching = [q for q in job.queries if q in incr_sync_cfg]
        if matching:
            logger.info("Incremental sync enabled for queries: %s", matching)

    # force_full_sync overrides both differential and incremental sync
    # (but not when fingerprint_only — we still need diff_config for L1 column info)
    if request.force_full_sync and not request.fingerprint_only:
        logger.info("force_full_sync=true: bypassing differential and incremental sync")
        diff_sync_cfg = None
        incr_sync_cfg = None

    background_tasks.add_task(
        _run_trigger_extraction, extractor, job, writer, db_client, jobs_tbl,
        tag, user.email, db_path, settings.max_parallel_queries,
        request.row_limit, request.lookback_months, diff_sync_cfg, incr_sync_cfg,
        request.fingerprint_only, request.table_suffix,
        request.suffix_exclude, request.force_full_sync, request.skip_phase2,
    )

    return TriggerResponse(
        job_id=job.job_id,
        status="pending",
        country=job.country,
        stage=request.stage,
        period=request.period,
        tag=tag,
        queries=job.queries,
        queries_count=len(job.queries),
        created_at=job.created_at,
        triggered_by=user.email,
    )


@router.get(
    "/events",
    response_model=EventListResponse,
    summary="List extraction events",
    description="List extraction events with optional filtering and pagination.",
)
async def list_events(
    user: CurrentAzureADUser,
    raw_request: Request,
    country: str | None = Query(default=None, description="Filter by country"),
    status_filter: JobStatus | None = Query(
        default=None, alias="status", description="Filter by status"
    ),
    stage: str | None = Query(default=None, description="Filter by stage"),
    period: str | None = Query(default=None, description="Filter by period (e.g. '202602')"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> EventListResponse:
    """List extraction events visible to the current user.

    Read order: in-memory (fastest) → SQLite (sub-ms) → Delta (slow, async fallback).
    """
    db_path = _get_db_path(raw_request)

    # --- Phase 1: Try SQLite as primary source (sub-millisecond) ---
    sqlite_jobs: list[dict] = []
    sqlite_total = 0
    if db_path:
        try:
            sqlite_jobs, sqlite_total = local_store.list_jobs(
                db_path,
                country=country,
                status=status_filter.value if status_filter else None,
                stage=stage,
                triggered_by=None if user.is_admin else user.email,
                period=period,
                limit=limit,
                offset=offset,
            )
        except Exception:
            logger.warning("SQLite list_jobs failed, will fall back to Delta", exc_info=True)

    if sqlite_jobs:
        # SQLite has data — build response from it, overlay in-memory live data
        items: list[EventSummary] = []
        seen_ids: set[str] = set()
        for row in sqlite_jobs:
            job_id = row["job_id"]
            seen_ids.add(job_id)
            record = _trigger_jobs.get(job_id)
            if record:
                # In-flight record: use live progress from memory
                job = record.job
                running_queries = sorted(record.running_queries)
                steps, current_step = _build_steps_for_job(job_id)
                items.append(EventSummary(
                    job_id=job_id,
                    status=job.status,
                    country=job.country,
                    stage=record.stage,
                    period=record.period,
                    tag=record.tag,
                    queries_total=len(job.queries),
                    queries_completed=job.queries_completed,
                    queries_failed=job.queries_failed,
                    queries_running=len(running_queries),
                    running_queries=running_queries,
                    failed_queries=[r.query_name for r in job.results if r.status == "failed"],
                    total_rows_extracted=sum(r.rows_extracted or 0 for r in job.results),
                    created_at=job.created_at,
                    started_at=job.started_at,
                    completed_at=job.completed_at,
                    triggered_by=record.triggered_by,
                    error=job.error,
                    current_query=job.current_query,
                    steps=steps,
                    current_step=current_step,
                ))
            else:
                # No in-flight record: build from SQLite row
                items.append(_build_event_summary(row, _results_from_row(row)))

        # Include in-flight jobs not yet in SQLite
        for job_id, record in _trigger_jobs.items():
            if job_id in seen_ids:
                continue
            job = record.job
            if country and job.country != country:
                continue
            if status_filter and job.status != status_filter:
                continue
            if stage and record.stage != stage:
                continue
            if period and record.period != period:
                continue
            if not user.is_admin and record.triggered_by != user.email:
                continue
            inflight_running = sorted(record.running_queries)
            inflight_steps, inflight_current = _build_steps_for_job(job_id)
            items.insert(0, EventSummary(
                job_id=job_id,
                status=job.status,
                country=job.country,
                stage=record.stage,
                period=record.period,
                tag=record.tag,
                queries_total=len(job.queries),
                queries_completed=job.queries_completed,
                queries_failed=job.queries_failed,
                queries_running=len(inflight_running),
                running_queries=inflight_running,
                failed_queries=[r.query_name for r in job.results if r.status == "failed"],
                total_rows_extracted=sum(r.rows_extracted or 0 for r in job.results),
                created_at=job.created_at,
                started_at=job.started_at,
                completed_at=job.completed_at,
                triggered_by=record.triggered_by,
                error=job.error,
                current_query=job.current_query,
                steps=inflight_steps,
                current_step=inflight_current,
            ))
            sqlite_total += 1

        return EventListResponse(items=items, total=sqlite_total, limit=limit, offset=offset)

    # --- Phase 2: SQLite empty — fall back to Delta (cold start path) ---
    db_client = _get_databricks_client(raw_request)
    jobs_tbl = get_settings().jobs_table
    try:
        db_jobs, total = list_jobs(
            db_client,
            jobs_tbl,
            country=country,
            status=status_filter.value if status_filter else None,
            stage=stage,
            triggered_by=None if user.is_admin else user.email,
            period=period,
            limit=limit,
            offset=offset,
        )
    except Exception:
        logger.error("Failed to query jobs from Delta table", exc_info=True)
        db_jobs, total = [], 0

    items = []
    db_job_ids = set()
    for row in db_jobs:
        db_job_ids.add(row["job_id"])
        record = _trigger_jobs.get(row["job_id"])
        if record:
            job = record.job
            running_queries = sorted(record.running_queries)
            total_rows = sum(r.rows_extracted or 0 for r in job.results)
            steps, current_step = _build_steps_for_job(row["job_id"])
            items.append(EventSummary(
                job_id=row["job_id"],
                status=job.status,
                country=row["country"],
                stage=row["stage"],
                period=row.get("period"),
                tag=row["tag"],
                queries_total=len(row["queries"]),
                queries_completed=job.queries_completed,
                queries_failed=job.queries_failed,
                queries_running=len(running_queries),
                running_queries=running_queries,
                failed_queries=[r.query_name for r in job.results if r.status == "failed"],
                total_rows_extracted=total_rows,
                created_at=row["created_at"],
                started_at=job.started_at,
                completed_at=job.completed_at,
                triggered_by=row["triggered_by"],
                error=job.error,
                current_query=job.current_query,
                steps=steps,
                current_step=current_step,
            ))
        else:
            steps, current_step = _build_steps_for_job(row["job_id"])
            items.append(EventSummary(
                job_id=row["job_id"],
                status=row["status"],
                country=row["country"],
                stage=row["stage"],
                period=row.get("period"),
                tag=row["tag"],
                queries_total=len(row["queries"]),
                queries_completed=0,
                queries_failed=0,
                queries_running=0,
                running_queries=[],
                failed_queries=[],
                total_rows_extracted=0,
                created_at=row["created_at"],
                started_at=row.get("started_at"),
                completed_at=row.get("completed_at"),
                triggered_by=row["triggered_by"],
                error=row.get("error"),
                steps=steps,
                current_step=current_step,
            ))

    # Include in-flight jobs not yet in Delta
    for job_id, record in _trigger_jobs.items():
        if job_id in db_job_ids:
            continue
        job = record.job
        if country and job.country != country:
            continue
        if status_filter and job.status != status_filter:
            continue
        if stage and record.stage != stage:
            continue
        if period and record.period != period:
            continue
        if not user.is_admin and record.triggered_by != user.email:
            continue
        inflight_running = sorted(record.running_queries)
        inflight_steps, inflight_current = _build_steps_for_job(job_id)
        items.insert(0, EventSummary(
            job_id=job_id,
            status=job.status,
            country=job.country,
            stage=record.stage,
            period=record.period,
            tag=record.tag,
            queries_total=len(job.queries),
            queries_completed=job.queries_completed,
            queries_failed=job.queries_failed,
            queries_running=len(inflight_running),
            running_queries=inflight_running,
            failed_queries=[r.query_name for r in job.results if r.status == "failed"],
            total_rows_extracted=sum(r.rows_extracted or 0 for r in job.results),
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            triggered_by=record.triggered_by,
            error=job.error,
            current_query=job.current_query,
            steps=inflight_steps,
            current_step=inflight_current,
        ))
        total += 1

    return EventListResponse(items=items, total=total, limit=limit, offset=offset)


@router.get(
    "/events/{job_id}",
    response_model=EventDetail,
    summary="Get event detail",
    description="Get detailed status of a specific extraction event.",
)
async def get_event_detail(
    job_id: str,
    user: CurrentAzureADUser,
    raw_request: Request,
) -> EventDetail:
    """Get detailed event info including per-query progress.

    Read order: in-memory → SQLite → Delta (only if needed).
    """
    db_path = _get_db_path(raw_request)

    # 1) Check in-flight record first (fastest, avoids any I/O)
    record = _trigger_jobs.get(job_id)
    if record:
        if not user.is_admin and record.triggered_by != user.email:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error": "not_found", "message": f"Job not found: {job_id}"},
            )
        job = record.job
        inflight_running = sorted(record.running_queries)
        detail_steps, detail_current = _build_steps_for_job(job.job_id)
        return EventDetail(
            job_id=job.job_id,
            status=job.status,
            country=job.country,
            stage=record.stage,
            period=record.period,
            tag=record.tag,
            queries_total=len(job.queries),
            queries_completed=job.queries_completed,
            queries_failed=job.queries_failed,
            queries_running=len(inflight_running),
            running_queries=inflight_running,
            total_rows_extracted=sum(r.rows_extracted or 0 for r in job.results),
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            triggered_by=record.triggered_by,
            error=job.error,
            current_query=job.current_query,
            steps=detail_steps,
            current_step=detail_current,
            results=job.results,
        )

    # 2) Try SQLite (sub-ms)
    sqlite_row = None
    if db_path:
        try:
            sqlite_row = local_store.get_job(db_path, job_id)
        except Exception:
            pass

    if sqlite_row:
        if not user.is_admin and sqlite_row["triggered_by"] != user.email:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error": "not_found", "message": f"Job not found: {job_id}"},
            )
        sqlite_results = _results_from_row(sqlite_row)
        completed_count, failed_count = _compute_counts(sqlite_results)
        rq = sqlite_row.get("running_queries", [])
        running_queries = rq if isinstance(rq, list) else []
        sq_steps, sq_current = _build_steps_for_job(job_id, db_path)
        return EventDetail(
            job_id=job_id,
            status=sqlite_row["status"],
            country=sqlite_row["country"],
            stage=sqlite_row["stage"],
            period=sqlite_row.get("period"),
            tag=sqlite_row["tag"],
            queries_total=len(sqlite_row.get("queries", [])),
            queries_completed=completed_count,
            queries_failed=failed_count,
            queries_running=len(running_queries),
            running_queries=running_queries,
            total_rows_extracted=_total_rows(sqlite_results),
            created_at=sqlite_row["created_at"],
            started_at=sqlite_row.get("started_at"),
            completed_at=sqlite_row.get("completed_at"),
            triggered_by=sqlite_row["triggered_by"],
            error=sqlite_row.get("error"),
            current_query=sqlite_row.get("current_query"),
            steps=sq_steps,
            current_step=sq_current,
            results=sqlite_results,
        )

    # 3) Fall back to Delta (slow path - only for old historical jobs not in SQLite)
    db_client = _get_databricks_client(raw_request)
    jobs_tbl = get_settings().jobs_table
    db_row = get_job(db_client, jobs_tbl, job_id)

    if not db_row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Job not found: {job_id}"},
        )

    if not user.is_admin and db_row["triggered_by"] != user.email:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Job not found: {job_id}"},
        )

    completed_steps, completed_current = _build_steps_for_job(db_row["job_id"], db_path)
    return EventDetail(
        job_id=db_row["job_id"],
        status=db_row["status"],
        country=db_row["country"],
        stage=db_row["stage"],
        period=db_row.get("period"),
        tag=db_row["tag"],
        queries_total=len(db_row["queries"]),
        queries_completed=0,
        queries_failed=0,
        queries_running=0,
        running_queries=[],
        total_rows_extracted=0,
        created_at=db_row["created_at"],
        started_at=db_row.get("started_at"),
        completed_at=db_row.get("completed_at"),
        triggered_by=db_row["triggered_by"],
        error=db_row.get("error"),
        steps=completed_steps,
        current_step=completed_current,
        results=[],
    )


# --- Cancel job ---


@router.post(
    "/events/{job_id}/cancel",
    response_model=EventDetail,
    summary="Cancel a running job",
    description="Cancel a running sync or calibration job.",
)
async def cancel_event(
    job_id: str,
    user: CurrentAzureADUser,
    raw_request: Request,
) -> EventDetail:
    """Cancel a running job: mark status as cancelled, stop pending steps."""
    db_path = _get_db_path(raw_request)

    # Find the in-flight record
    record = _trigger_jobs.get(job_id)
    if record:
        if not user.is_admin and record.triggered_by != user.email:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error": "not_found", "message": f"Job not found: {job_id}"},
            )
        job = record.job
        if job.status not in (JobStatus.PENDING, JobStatus.RUNNING):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"error": "invalid_state", "message": f"Cannot cancel job with status: {job.status.value}"},
            )
        # Mark the job as cancelled
        job.status = JobStatus.CANCELLED
        job.completed_at = datetime.utcnow()
        job.error = "Cancelled by user"
        record.running_queries.clear()
    else:
        # Check SQLite for a persisted running job
        found_in_store = False
        if db_path:
            sqlite_row = local_store.get_job(db_path, job_id)
            if sqlite_row:
                found_in_store = True
                if not user.is_admin and sqlite_row["triggered_by"] != user.email:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail={"error": "not_found", "message": f"Job not found: {job_id}"},
                    )
                if sqlite_row["status"] not in ("pending", "running"):
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={"error": "invalid_state", "message": f"Cannot cancel job with status: {sqlite_row['status']}"},
                    )

        # Fall back to Databricks Delta (for jobs from previous server sessions)
        if not found_in_store:
            try:
                db_client = _get_databricks_client(raw_request)
                jobs_tbl = get_settings().jobs_table
                db_row = get_job(db_client, jobs_tbl, job_id)
            except Exception:
                db_row = None

            if not db_row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={"error": "not_found", "message": f"Job not found: {job_id}"},
                )
            if not user.is_admin and db_row["triggered_by"] != user.email:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={"error": "not_found", "message": f"Job not found: {job_id}"},
                )
            if db_row["status"] not in ("pending", "running"):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={"error": "invalid_state", "message": f"Cannot cancel job with status: {db_row['status']}"},
                )

    # Cancel calibration steps
    calibration_tracker.cancel_job(job_id)

    # Persist to SQLite
    if db_path:
        now_str = datetime.utcnow().isoformat()
        local_store.update_job(
            db_path, job_id,
            status="cancelled",
            completed_at=now_str,
            error="Cancelled by user",
            running_queries="[]",
            current_query=None,
        )
        _persist_steps(job_id, db_path)

    # Persist to Databricks (best-effort)
    try:
        db_client = _get_databricks_client(raw_request)
        jobs_tbl = get_settings().jobs_table
        update_job_status(
            db_client, jobs_tbl, job_id, "cancelled",
            completed_at=datetime.utcnow(),
            error="Cancelled by user",
        )
    except Exception as e:
        logger.warning("Failed to update Databricks job status for cancelled job %s: %s", job_id, e)

    logger.info("Job %s cancelled by %s", job_id, user.email)

    # Return updated detail
    return await get_event_detail(job_id, user, raw_request)


# --- Download CSV ---


def _collect_table_names(detail: EventDetail) -> list[str]:
    """Extract unique, non-empty table names from completed query results."""
    tables: list[str] = []
    seen: set[str] = set()
    for r in detail.results:
        if isinstance(r, QueryResult):
            r_status = r.status.value if hasattr(r.status, "value") else str(r.status)
            tbl = r.table_name
        elif isinstance(r, dict):
            r_status = r.get("status", "")
            tbl = r.get("table_name")
        else:
            continue
        if tbl and r_status == "completed" and tbl not in seen:
            tables.append(tbl)
            seen.add(tbl)
    return tables


class TableInfo(BaseModel):
    full_name: str = Field(description="Backtick-quoted fully qualified table name")
    catalog: str
    schema_name: str
    table_name: str


class TablesResponse(BaseModel):
    job_id: str
    country: str
    tables: list[TableInfo]


def _list_catalog_tables(db_client: DatabricksClient, country: str) -> list[TableInfo]:
    """List managed tables in 001-calibration-3-0 whose schema contains *country*."""
    if not re.fullmatch(r"[A-Za-z0-9_]+", country):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "invalid_country", "message": f"Invalid country value: {country}"},
        )
    sql = (
        "SELECT table_catalog, table_schema, table_name "
        "FROM `001-calibration-3-0`.information_schema.tables "
        f"WHERE table_schema LIKE '%{country}%' AND table_type = 'MANAGED' "
        "ORDER BY table_schema, table_name"
    )
    rows = db_client.execute_sql(sql)
    tables: list[TableInfo] = []
    for row in rows:
        catalog = row["table_catalog"]
        schema = row["table_schema"]
        tbl = row["table_name"]
        tables.append(
            TableInfo(
                full_name=f"`{catalog}`.`{schema}`.`{tbl}`",
                catalog=catalog,
                schema_name=schema,
                table_name=tbl,
            )
        )
    return tables


@router.get(
    "/events/{job_id}/tables",
    summary="List Databricks tables for a job's country",
    response_model=TablesResponse,
    responses={
        404: {"description": "Job not found"},
        502: {"description": "Databricks query failed"},
    },
)
async def list_event_tables(
    job_id: str,
    user: CurrentAzureADUser,
    raw_request: Request,
) -> TablesResponse:
    """List managed Delta tables in the calibration catalog for the job's country."""
    detail = await get_event_detail(job_id, user, raw_request)
    country = detail.country

    db_client = _get_databricks_client(raw_request)
    try:
        tables = _list_catalog_tables(db_client, country)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to list tables for country=%s job=%s: %s", country, job_id, exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={
                "error": "databricks_query_failed",
                "message": f"Could not list tables: {exc}",
            },
        ) from exc

    return TablesResponse(job_id=job_id, country=country, tables=tables)


@router.get(
    "/events/{job_id}/download",
    summary="Download job results as CSV",
    responses={
        200: {"content": {"text/csv": {}}, "description": "CSV file with extracted data"},
        404: {"description": "Job not found"},
        409: {"description": "Job not yet completed"},
    },
)
async def download_event_csv(
    job_id: str,
    user: CurrentAzureADUser,
    raw_request: Request,
    table: str | None = Query(
        default=None,
        description="Specific table to download. If omitted, downloads the largest result table.",
    ),
) -> StreamingResponse:
    """Download actual extracted data from Databricks as CSV.

    Reads the Delta table written during the calibration job and streams
    its contents as a CSV file.  When *table* is not specified, the result
    table with the most rows extracted is selected automatically.
    """
    detail = await get_event_detail(job_id, user, raw_request)

    job_status = detail.status if isinstance(detail.status, str) else detail.status.value

    all_steps_done = all(
        (s.status if isinstance(s.status, str) else s.status.value) in ("completed", "failed")
        for s in detail.steps
    ) if detail.steps else False

    if job_status not in ("completed", "failed") and not all_steps_done:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "job_not_finished",
                "message": f"Job is still {job_status}. Download available after completion.",
            },
        )

    # Determine which Delta table to read
    available_tables = _collect_table_names(detail)

    if table:
        # Caller requested a specific table — use it directly.
        # If the table doesn't exist, the Databricks query below will fail with 502,
        # and if it's empty, it will return 404 — both already handled.
        target_table = table
    elif available_tables:
        # Pick the table with the most rows extracted
        best: tuple[str, int] = (available_tables[0], 0)
        for r in detail.results:
            if isinstance(r, QueryResult):
                tbl, rows = r.table_name, r.rows_extracted
            elif isinstance(r, dict):
                tbl, rows = r.get("table_name"), r.get("rows_extracted", 0)
            else:
                continue
            if tbl and tbl in available_tables and (rows or 0) > best[1]:
                best = (tbl, rows or 0)
        target_table = best[0]
    else:
        # No tables in job results — try listing from the calibration catalog
        country = detail.country
        db_client = _get_databricks_client(raw_request)
        try:
            catalog_tables = _list_catalog_tables(db_client, country)
        except Exception:
            catalog_tables = []

        if catalog_tables:
            target_table = catalog_tables[0].full_name
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error": "no_tables",
                    "message": "No completed result tables available for this job.",
                },
            )

    # Query the Delta table from Databricks
    db_client = _get_databricks_client(raw_request)
    try:
        rows = db_client.execute_sql(f"SELECT * FROM {target_table}")
    except Exception as exc:
        logger.error("Failed to read table %s for job %s: %s", target_table, job_id, exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={
                "error": "databricks_read_failed",
                "message": f"Could not read table {target_table}: {exc}",
            },
        ) from exc

    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "empty_table",
                "message": f"Table {target_table} returned no rows.",
            },
        )

    columns = list(rows[0].keys())

    def _generate_csv():
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=columns)
        w.writeheader()
        yield buf.getvalue()
        buf.seek(0)
        buf.truncate(0)
        for row in rows:
            w.writerow(row)
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate(0)

    country = detail.country
    period = detail.period or "no-period"
    # Use only the table leaf name (last segment after the last dot)
    table_short = target_table.rsplit(".", 1)[-1].strip("`")
    filename = f"{table_short}_{country}_{period}_{job_id[:8]}.csv"

    return StreamingResponse(
        _generate_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
