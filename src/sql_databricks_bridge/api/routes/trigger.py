"""Trigger API endpoints -- frontend-facing sync trigger and event views."""

import csv
import io
import json
import logging
import os
import threading
import uuid as _uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

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
            results.append(QueryResult(**r))
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


def _launch_calibration_step(job_id: str, step_name: str, country: str) -> None:
    """Launch a Databricks job for a calibration step if a launcher is available."""
    if _calibration_launcher is None:
        logger.debug("No calibration launcher configured; skipping launch for %s/%s", job_id, step_name)
        return
    try:
        _calibration_launcher.launch_step(job_id, step_name, country)
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
) -> None:
    """Background task to run the extraction triggered by a user."""
    try:
        import polars as pl

        settings = get_settings()
        version_tags_tbl = settings.version_tags_table

        job.status = JobStatus.RUNNING
        job.started_at = datetime.utcnow()
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

        def process_query(query_name: str) -> QueryResult:
            """Process a single query — runs inside ThreadPoolExecutor."""
            # Skip if job was cancelled
            if job.status == JobStatus.CANCELLED:
                return QueryResult(query_name=query_name, status=JobStatus.CANCELLED)

            with results_lock:
                if record:
                    record.running_queries.add(query_name)
                _persist_progress()

            result = QueryResult(query_name=query_name, status=JobStatus.RUNNING)
            start_time = datetime.utcnow()

            try:
                row_limit = override_row_limit if override_row_limit is not None else (settings.query_row_limit or None)
                lookback = override_lookback_months if override_lookback_months is not None else settings.lookback_months
                chunks = list(
                    extractor.execute_query(query_name, job.country, job.chunk_size, limit=row_limit, lookback_months=lookback)
                )

                if chunks:
                    combined = concat_chunks(chunks)
                    writer.write_dataframe(combined, query_name, job.country, tag=tag)
                    result.rows_extracted = len(combined)
                    result.table_name = writer.resolve_table_name(
                        query_name, job.country
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
                else:
                    result.rows_extracted = 0

                result.status = JobStatus.COMPLETED

            except Exception as e:
                result.status = JobStatus.FAILED
                result.error = str(e)
                logger.error(f"Query {query_name} failed: {e}")

            result.duration_seconds = (
                datetime.utcnow() - start_time
            ).total_seconds()

            with results_lock:
                if record:
                    record.running_queries.discard(query_name)
                job.results.append(result)
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
        # Don't override if already cancelled by user
        if job.status != JobStatus.CANCELLED:
            all_sync_failed = job.queries_failed > 0 and job.queries_completed == 0
            if all_sync_failed:
                job.status = JobStatus.FAILED
                job.completed_at = job.completed_at or datetime.utcnow()
            else:
                # Calibration steps remain: keep status as RUNNING so the
                # frontend continues polling until the monitor finalizes.
                job.status = JobStatus.RUNNING
                # Don't set completed_at yet -- _finalize_job handles that.

        # Persist extraction results (status stays "running" for successful sync)
        persist_status = job.status.value
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
                _launch_calibration_step(job.job_id, next_step, job.country)

    except Exception as e:
        job.current_query = None
        job.status = JobStatus.FAILED
        job.error = str(e)
        logger.error(f"Trigger job {job.job_id} failed: {e}")
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
        calibration_tracker.create(job_id, country=request.country)
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
            _launch_calibration_step(job_id, next_step, request.country)

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
        queries_path = get_settings().queries_path
        loader = CountryAwareQueryLoader(queries_path)
        if loader.is_server(request.country):
            sql_client = SQLServerClient(server=request.country, database="master")
        else:
            sql_client = SQLServerClient(country=request.country)
        extractor = Extractor(
            queries_path=queries_path,
            sql_client=sql_client,
        )

        job = extractor.create_job(
            country=request.country,
            destination="",
            queries=request.queries,
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

    # Persist to Databricks Delta table
    aggregations_dict = request.aggregations.model_dump() if request.aggregations else None
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
    calibration_tracker.create(job.job_id, country=request.country)
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
    background_tasks.add_task(
        _run_trigger_extraction, extractor, job, writer, db_client, jobs_tbl,
        tag, user.email, db_path, settings.max_parallel_queries,
        request.row_limit, request.lookback_months,
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
        if db_path:
            sqlite_row = local_store.get_job(db_path, job_id)
            if sqlite_row:
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
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={"error": "not_found", "message": f"Job not found: {job_id}"},
                )
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error": "not_found", "message": f"Job not found: {job_id}"},
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


@router.get(
    "/events/{job_id}/download",
    summary="Download job results as CSV",
    responses={
        200: {"content": {"text/csv": {}}, "description": "CSV file"},
        404: {"description": "Job not found"},
        409: {"description": "Job not yet completed"},
    },
)
async def download_event_csv(
    job_id: str,
    user: CurrentAzureADUser,
    raw_request: Request,
) -> StreamingResponse:
    """Generate and return a CSV report for a completed calibration job."""
    # Reuse get_event_detail to gather all data
    detail = await get_event_detail(job_id, user, raw_request)

    job_status = detail.status if isinstance(detail.status, str) else detail.status.value

    # Also allow download when all steps are completed (overall status may lag)
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

    output = io.StringIO()
    writer = csv.writer(output)

    # Section 1: Job metadata
    writer.writerow(["# Calibration Job Report"])
    writer.writerow(["job_id", job_id])
    writer.writerow(["country", detail.country])
    writer.writerow(["stage", detail.stage])
    writer.writerow(["period", detail.period or ""])
    writer.writerow(["tag", detail.tag])
    writer.writerow(["status", job_status])
    writer.writerow(["triggered_by", detail.triggered_by])
    writer.writerow(["started_at", str(detail.started_at) if detail.started_at else ""])
    writer.writerow(["completed_at", str(detail.completed_at) if detail.completed_at else ""])
    writer.writerow(["error", detail.error or ""])
    writer.writerow([])

    # Section 2: Pipeline steps
    writer.writerow(["# Pipeline Steps"])
    writer.writerow(["step_name", "status", "started_at", "completed_at", "duration_seconds", "error"])
    for s in detail.steps:
        s_status = s.status if isinstance(s.status, str) else s.status.value
        s_start = str(s.started_at) if s.started_at else ""
        s_end = str(s.completed_at) if s.completed_at else ""
        duration = ""
        if s.started_at and s.completed_at:
            try:
                duration = str(round((s.completed_at - s.started_at).total_seconds(), 2))
            except Exception:
                pass
        writer.writerow([s.name, s_status, s_start, s_end, duration, s.error or ""])
    writer.writerow([])

    # Section 3: Query results
    writer.writerow(["# Query Results"])
    writer.writerow(["query_name", "status", "rows_extracted", "table_name", "duration_seconds", "error"])
    for r in detail.results:
        if isinstance(r, QueryResult):
            r_status = r.status.value if hasattr(r.status, "value") else str(r.status)
            writer.writerow([
                r.query_name, r_status, r.rows_extracted,
                r.table_name or "", round(r.duration_seconds, 2),
                r.error or "",
            ])
        elif isinstance(r, dict):
            writer.writerow([
                r.get("query_name", ""), r.get("status", ""),
                r.get("rows_extracted", 0), r.get("table_name", ""),
                round(r.get("duration_seconds", 0), 2), r.get("error", ""),
            ])

    csv_content = output.getvalue()
    output.close()

    country = detail.country
    period = detail.period or "no-period"
    filename = f"calibration_{country}_{period}_{job_id[:8]}.csv"

    return StreamingResponse(
        iter([csv_content]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
