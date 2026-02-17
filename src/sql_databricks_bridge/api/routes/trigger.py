"""Trigger API endpoints -- frontend-facing sync trigger and event views."""

import json
import logging
import os
import threading
import uuid as _uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from sql_databricks_bridge.api.dependencies import CurrentAzureADUser
from sql_databricks_bridge.api.schemas import JobStatus, QueryResult
from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.core.delta_writer import DeltaTableWriter
from sql_databricks_bridge.core.country_query_loader import CountryAwareQueryLoader
from sql_databricks_bridge.core.extractor import Extractor, ExtractionJob
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


class CalibrationStepResponse(BaseModel):
    """Calibration step as expected by the calibration frontend."""

    name: str
    status: str
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None


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


def _build_steps_for_job(job_id: str) -> tuple[list[CalibrationStepResponse] | None, str | None]:
    """Get calibration steps and current_step for a job."""
    steps_data = calibration_tracker.get_steps_for_response(job_id)
    current = calibration_tracker.get_current_step(job_id)
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
            with results_lock:
                if record:
                    record.running_queries.add(query_name)
                _persist_progress()

            result = QueryResult(query_name=query_name, status=JobStatus.RUNNING)
            start_time = datetime.utcnow()

            try:
                row_limit = settings.query_row_limit or None
                lookback = settings.lookback_months
                chunks = list(
                    extractor.execute_query(query_name, job.country, job.chunk_size, limit=row_limit, lookback_months=lookback)
                )

                if chunks:
                    combined = pl.concat(chunks)
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
        job.status = (
            JobStatus.FAILED
            if job.queries_failed > 0 and job.queries_completed == 0
            else JobStatus.COMPLETED
        )
        job.completed_at = datetime.utcnow()
        update_job_status(
            client,
            table,
            job.job_id,
            job.status.value,
            completed_at=job.completed_at,
        )
        if db_path:
            serialized_results = json.dumps(
                [r.model_dump(mode="json") for r in job.results]
            )
            local_store.update_job(
                db_path,
                job.job_id,
                status=job.status.value,
                completed_at=job.completed_at.isoformat(),
                current_query=None,
                running_queries="[]",
                results=serialized_results,
            )

        # Update calibration sync_data step
        if job.queries_failed > 0 and job.queries_completed == 0:
            calibration_tracker.complete_step(
                job.job_id,
                "sync_data",
                error=f"{job.queries_failed} query(ies) failed during sync",
            )
        else:
            calibration_tracker.complete_step(job.job_id, "sync_data")
            # Auto-advance to copy_to_calibration
            next_step = calibration_tracker.advance_after_sync(job.job_id)
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


# --- Endpoints ---


@router.post(
    "/trigger",
    response_model=TriggerResponse,
    status_code=status.HTTP_202_ACCEPTED,
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

    # Check for skip-sync mode (for e2e testing without SQL Server)
    skip_sync = os.environ.get("SKIP_SYNC_DATA", "").lower() in ("1", "true", "yes")

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

        # Persist to Databricks Delta table
        aggregations_dict = request.aggregations.model_dump() if request.aggregations else None
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

        # Create calibration tracking - immediately complete sync_data
        calibration_tracker.create(job_id, country=request.country)
        calibration_tracker.start_step(job_id, "sync_data")
        calibration_tracker.complete_step(job_id, "sync_data")

        # Advance to copy_to_calibration and launch Databricks job
        next_step = calibration_tracker.advance_after_sync(job_id)
        if next_step:
            _launch_calibration_step(job_id, next_step, request.country)

        logger.info("SKIP_SYNC_DATA: job %s created, sync_data skipped, advancing to %s", job_id, next_step)

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

    # Persist to local SQLite for crash recovery
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
            )
        except Exception as sqlite_err:
            logger.warning(f"Failed to insert job into SQLite: {sqlite_err}")

    # Create calibration step tracking
    calibration_tracker.create(job.job_id, country=request.country)
    calibration_tracker.start_step(job.job_id, "sync_data")

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
    """List extraction events visible to the current user."""
    db_client = _get_databricks_client(raw_request)
    jobs_tbl = get_settings().jobs_table
    db_path = _get_db_path(raw_request)

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

    if not db_jobs and total == 0:
        logger.debug("No Delta table results found for list_events query")

    items: list[EventSummary] = []
    db_job_ids = set()
    for row in db_jobs:
        db_job_ids.add(row["job_id"])
        # If there's an in-flight record, use live progress data
        record = _trigger_jobs.get(row["job_id"])
        if record:
            job = record.job
            queries_completed = job.queries_completed
            queries_failed = job.queries_failed
            live_status = job.status
            started_at = job.started_at
            completed_at = job.completed_at
            error = job.error
            current_query = job.current_query
            running_queries = sorted(record.running_queries)
            total_rows = sum(r.rows_extracted or 0 for r in job.results)
        else:
            # Try SQLite for live state on running jobs
            sqlite_row = None
            if db_path and row.get("status") in ("running", "pending"):
                try:
                    sqlite_row = local_store.get_job(db_path, row["job_id"])
                except Exception:
                    pass
            if sqlite_row:
                sqlite_results = _results_from_row(sqlite_row)
                c, f = _compute_counts(sqlite_results)
                queries_completed = c
                queries_failed = f
                live_status = sqlite_row["status"]
                started_at = sqlite_row.get("started_at")
                completed_at = sqlite_row.get("completed_at")
                error = sqlite_row.get("error")
                current_query = sqlite_row.get("current_query")
                rq = sqlite_row.get("running_queries", [])
                running_queries = rq if isinstance(rq, list) else []
                total_rows = _total_rows(sqlite_results)
            else:
                queries_completed = 0
                queries_failed = 0
                live_status = row["status"]
                started_at = row.get("started_at")
                completed_at = row.get("completed_at")
                error = row.get("error")
                current_query = None
                running_queries = []
                total_rows = 0

        steps, current_step = _build_steps_for_job(row["job_id"])
        items.append(
            EventSummary(
                job_id=row["job_id"],
                status=live_status,
                country=row["country"],
                stage=row["stage"],
                period=row.get("period"),
                tag=row["tag"],
                queries_total=len(row["queries"]),
                queries_completed=queries_completed,
                queries_failed=queries_failed,
                queries_running=len(running_queries),
                running_queries=running_queries,
                total_rows_extracted=total_rows,
                created_at=row["created_at"],
                started_at=started_at,
                completed_at=completed_at,
                triggered_by=row["triggered_by"],
                error=error,
                current_query=current_query,
                steps=steps,
                current_step=current_step,
            )
        )

    # Include in-flight jobs not yet visible in the Delta table
    for job_id, record in _trigger_jobs.items():
        if job_id in db_job_ids:
            continue
        job = record.job
        # Apply filters
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
        inflight_rows = sum(r.rows_extracted or 0 for r in job.results)
        inflight_steps, inflight_current = _build_steps_for_job(job.job_id)
        items.insert(
            0,
            EventSummary(
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
                total_rows_extracted=inflight_rows,
                created_at=job.created_at,
                started_at=job.started_at,
                completed_at=job.completed_at,
                triggered_by=record.triggered_by,
                error=job.error,
                current_query=job.current_query,
                steps=inflight_steps,
                current_step=inflight_current,
            ),
        )
        total += 1

    return EventListResponse(
        items=items,
        total=total,
        limit=limit,
        offset=offset,
    )


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
    """Get detailed event info including per-query progress."""
    db_client = _get_databricks_client(raw_request)
    jobs_tbl = get_settings().jobs_table
    db_path = _get_db_path(raw_request)

    # Check in-flight record first (avoids Delta table read-after-write lag)
    record = _trigger_jobs.get(job_id)

    db_row = get_job(db_client, jobs_tbl, job_id)

    # Also try SQLite for crash-recovered or live state
    sqlite_row = None
    if db_path:
        try:
            sqlite_row = local_store.get_job(db_path, job_id)
        except Exception:
            pass

    if not db_row and not record and not sqlite_row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Job not found: {job_id}"},
        )

    # Non-admin users can only see their own jobs
    triggered_by = (
        record.triggered_by if record
        else db_row["triggered_by"] if db_row
        else sqlite_row["triggered_by"]
    )
    if not user.is_admin and triggered_by != user.email:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Job not found: {job_id}"},
        )

    # If there's an in-flight record, use live progress
    if record:
        job = record.job
        # Prefer db_row for stage/tag/period if available, fallback to in-flight data
        detail_stage = db_row["stage"] if db_row else record.stage
        detail_tag = db_row["tag"] if db_row else record.tag
        detail_period = db_row.get("period") if db_row else record.period
        inflight_running = sorted(record.running_queries)
        inflight_rows = sum(r.rows_extracted or 0 for r in job.results)
        detail_steps, detail_current = _build_steps_for_job(job.job_id)
        return EventDetail(
            job_id=job.job_id,
            status=job.status,
            country=job.country,
            stage=detail_stage,
            period=detail_period,
            tag=detail_tag,
            queries_total=len(job.queries),
            queries_completed=job.queries_completed,
            queries_failed=job.queries_failed,
            queries_running=len(inflight_running),
            running_queries=inflight_running,
            total_rows_extracted=inflight_rows,
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

    # Try SQLite for richer detail (results, running_queries)
    if sqlite_row:
        sqlite_results = _results_from_row(sqlite_row)
        completed_count, failed_count = _compute_counts(sqlite_results)
        rq = sqlite_row.get("running_queries", [])
        running_queries = rq if isinstance(rq, list) else []
        row_total = _total_rows(sqlite_results)
        # Use db_row fields where available, fallback to sqlite_row
        source = db_row if db_row else sqlite_row
        sq_steps, sq_current = _build_steps_for_job(job_id)
        return EventDetail(
            job_id=job_id,
            status=sqlite_row["status"],
            country=source["country"],
            stage=source["stage"],
            period=source.get("period") if db_row else None,
            tag=source["tag"],
            queries_total=len(source.get("queries", [])),
            queries_completed=completed_count,
            queries_failed=failed_count,
            queries_running=len(running_queries),
            running_queries=running_queries,
            total_rows_extracted=row_total,
            created_at=source["created_at"],
            started_at=sqlite_row.get("started_at") or source.get("started_at"),
            completed_at=sqlite_row.get("completed_at") or source.get("completed_at"),
            triggered_by=source["triggered_by"],
            error=sqlite_row.get("error") or source.get("error"),
            current_query=sqlite_row.get("current_query"),
            steps=sq_steps,
            current_step=sq_current,
            results=sqlite_results,
        )

    # Job completed and no longer in-flight -- return from Databricks only
    completed_steps, completed_current = _build_steps_for_job(db_row["job_id"])
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
