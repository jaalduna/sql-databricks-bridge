"""Trigger API endpoints -- frontend-facing sync trigger and event views."""

import json
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from sql_databricks_bridge.api.dependencies import CurrentAzureADUser
from sql_databricks_bridge.api.schemas import JobStatus, QueryResult
from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.core.delta_writer import DeltaTableWriter
from sql_databricks_bridge.core.extractor import Extractor, ExtractionJob
from sql_databricks_bridge.core.stages import build_tag
from sql_databricks_bridge.db import local_store
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.jobs_table import (
    get_job as delta_get_job,
    insert_job as delta_insert_job,
    list_jobs as delta_list_jobs,
    update_job_status,
)
from sql_databricks_bridge.db.sql_server import SQLServerClient

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Trigger"])


# --- Schemas ---


class TriggerRequest(BaseModel):
    country: str = Field(..., description="Country code (e.g. 'bolivia', 'brazil')")
    stage: str = Field(..., description="Stage code (e.g. 'calibracion', 'mtr')")
    queries: list[str] | None = Field(
        default=None,
        description="Specific queries to run. null = all queries for the country.",
    )


class TriggerResponse(BaseModel):
    job_id: str
    status: str
    country: str
    stage: str
    tag: str
    queries: list[str]
    queries_count: int
    created_at: datetime
    triggered_by: str


class EventSummary(BaseModel):
    job_id: str
    status: JobStatus
    country: str
    stage: str
    tag: str
    queries_total: int
    queries_completed: int
    queries_failed: int
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    triggered_by: str
    error: str | None = None
    current_query: str | None = None
    failed_queries: list[str] = Field(default_factory=list)
    running_queries: list[str] = Field(default_factory=list)
    queries_running: int = 0
    total_rows_extracted: int = 0


class EventDetail(EventSummary):
    results: list[QueryResult] = Field(default_factory=list)


class EventListResponse(BaseModel):
    items: list[EventSummary]
    total: int
    limit: int
    offset: int


# --- Helpers ---


def _get_databricks_client(request: Request) -> DatabricksClient:
    """Get DatabricksClient from app.state, or create a new one as fallback."""
    client = getattr(request.app.state, "databricks_client", None)
    if client is not None:
        return client
    return DatabricksClient()


def _get_db_path(request: Request) -> str:
    """Get the SQLite database path from app.state."""
    return getattr(request.app.state, "sqlite_db_path", get_settings().sqlite_db_path)


def _results_from_row(row: dict) -> list[QueryResult]:
    """Deserialize results JSON from a SQLite row into QueryResult objects."""
    raw = row.get("results", [])
    if not raw:
        return []
    return [QueryResult(**r) for r in raw]


def _compute_counts(results: list[QueryResult]) -> tuple[int, int]:
    """Return (queries_completed, queries_failed) from a results list."""
    completed = sum(1 for r in results if r.status == JobStatus.COMPLETED)
    failed = sum(1 for r in results if r.status == JobStatus.FAILED)
    return completed, failed


def _total_rows(results: list[QueryResult]) -> int:
    """Sum rows_extracted across all results."""
    return sum(r.rows_extracted for r in results)


def _build_event_summary(row: dict, results: list[QueryResult] | None = None) -> EventSummary:
    """Build an EventSummary from a SQLite row, computing aggregates from results."""
    if results is None:
        results = _results_from_row(row)
    q_completed, q_failed = _compute_counts(results)
    running_queries = row.get("running_queries", [])

    return EventSummary(
        job_id=row["job_id"],
        status=row["status"],
        country=row["country"],
        stage=row["stage"],
        tag=row["tag"],
        queries_total=len(row["queries"]),
        queries_completed=q_completed,
        queries_failed=q_failed,
        created_at=row["created_at"],
        started_at=row.get("started_at"),
        completed_at=row.get("completed_at"),
        triggered_by=row["triggered_by"],
        error=row.get("error"),
        current_query=row.get("current_query"),
        failed_queries=row.get("failed_queries", []),
        running_queries=running_queries,
        queries_running=len(running_queries),
        total_rows_extracted=_total_rows(results),
    )


# --- Background task ---


def _run_trigger_extraction(
    extractor: Extractor,
    job: ExtractionJob,
    writer: DeltaTableWriter,
    client: DatabricksClient,
    table: str,
    db_path: str,
    max_parallel: int,
) -> None:
    """Background task: run queries in parallel, persist progress to SQLite."""
    try:
        import polars as pl

        job.status = JobStatus.RUNNING
        job.started_at = datetime.utcnow()
        local_store.update_job(
            db_path, job.job_id,
            status="running",
            started_at=job.started_at.isoformat(),
        )
        try:
            update_job_status(client, table, job.job_id, "running", started_at=job.started_at)
        except Exception:
            logger.warning(f"Failed to update Delta table for job {job.job_id} (running)")

        results_lock = threading.Lock()
        running_set: set[str] = set()

        def _persist_progress() -> None:
            """Persist current progress to SQLite (call under results_lock)."""
            local_store.update_job(
                db_path, job.job_id,
                current_query=next(iter(running_set), None),
                running_queries=json.dumps(sorted(running_set)),
                results=json.dumps([r.model_dump(mode="json") for r in job.results]),
                failed_queries=json.dumps(
                    [r.query_name for r in job.results if r.status == JobStatus.FAILED]
                ),
            )

        def process_query(query_name: str) -> QueryResult:
            """Run one query (SQL read -> Databricks write). Runs in thread."""
            start_time = datetime.utcnow()

            # Mark query as running
            with results_lock:
                running_set.add(query_name)
                _persist_progress()

            result = QueryResult(
                query_name=query_name,
                status=JobStatus.RUNNING,
                started_at=start_time,
            )
            try:
                row_limit = int(os.environ.get("QUERY_ROW_LIMIT", "0")) or None
                chunks = list(
                    extractor.execute_query(query_name, job.country, job.chunk_size, limit=row_limit)
                )
                if chunks:
                    combined = pl.concat(chunks)
                    writer.write_dataframe(combined, query_name, job.country)
                    result.rows_extracted = len(combined)
                    result.table_name = writer.resolve_table_name(query_name, job.country)
                else:
                    result.rows_extracted = 0
                result.status = JobStatus.COMPLETED
            except Exception as e:
                result.status = JobStatus.FAILED
                result.error = str(e)
                logger.error(f"Query {query_name} failed: {e}")
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()

            # Mark query as done, persist progress
            with results_lock:
                running_set.discard(query_name)
                job.results.append(result)
                _persist_progress()

            return result

        with ThreadPoolExecutor(max_workers=max_parallel) as pool:
            futures = {pool.submit(process_query, q): q for q in job.queries}
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    query_name = futures[future]
                    logger.error(f"Unexpected thread error for query {query_name}: {exc}")

        # Final status
        job.current_query = None
        job.status = (
            JobStatus.FAILED
            if job.queries_failed > 0 and job.queries_completed == 0
            else JobStatus.COMPLETED
        )
        job.completed_at = datetime.utcnow()
        failed_query_names = [r.query_name for r in job.results if r.status == JobStatus.FAILED]

        local_store.update_job(
            db_path, job.job_id,
            status=job.status.value,
            completed_at=job.completed_at.isoformat(),
            current_query=None,
            running_queries=json.dumps([]),
            failed_queries=json.dumps(failed_query_names),
            results=json.dumps([r.model_dump(mode="json") for r in job.results]),
        )
        try:
            update_job_status(
                client, table, job.job_id, job.status.value,
                completed_at=job.completed_at, failed_queries=failed_query_names,
            )
        except Exception:
            logger.warning(f"Failed to update Delta table for job {job.job_id} (final)")

    except Exception as e:
        job.current_query = None
        job.status = JobStatus.FAILED
        job.error = str(e)
        logger.error(f"Trigger job {job.job_id} failed: {e}")
        local_store.update_job(
            db_path, job.job_id,
            status="failed",
            error=str(e),
            completed_at=datetime.utcnow().isoformat(),
            running_queries=json.dumps([]),
        )
        try:
            update_job_status(client, table, job.job_id, "failed", error=str(e))
        except Exception:
            logger.warning(f"Failed to update Delta table for job {job.job_id} (error)")


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

    # Create extractor and job
    try:
        sql_client = SQLServerClient(country=request.country)
        extractor = Extractor(
            queries_path=get_settings().queries_path,
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
    db_path = _get_db_path(raw_request)

    # Persist to Databricks Delta table (best-effort audit trail)
    try:
        delta_insert_job(
            db_client,
            jobs_tbl,
            job_id=job.job_id,
            country=request.country,
            stage=request.stage,
            tag=tag,
            queries=job.queries,
            triggered_by=user.email,
            created_at=job.created_at,
        )
    except Exception:
        logger.warning(f"Failed to insert job {job.job_id} into Delta table")

    # Persist to local SQLite (primary store)
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

    # Launch background extraction
    writer = DeltaTableWriter(db_client)
    background_tasks.add_task(
        _run_trigger_extraction,
        extractor, job, writer, db_client, jobs_tbl,
        db_path, settings.max_parallel_queries,
    )

    return TriggerResponse(
        job_id=job.job_id,
        status="pending",
        country=job.country,
        stage=request.stage,
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
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> EventListResponse:
    """List extraction events visible to the current user."""
    db_path = _get_db_path(raw_request)

    sqlite_jobs, total = local_store.list_jobs(
        db_path,
        country=country,
        status=status_filter.value if status_filter else None,
        triggered_by=None if user.is_admin else user.email,
        limit=limit,
        offset=offset,
    )

    items: list[EventSummary] = []
    sqlite_job_ids = set()

    for row in sqlite_jobs:
        sqlite_job_ids.add(row["job_id"])
        items.append(_build_event_summary(row))

    # Fallback: include Delta table jobs not in SQLite (pre-migration)
    try:
        db_client = _get_databricks_client(raw_request)
        jobs_tbl = get_settings().jobs_table
        delta_jobs, delta_total = delta_list_jobs(
            db_client,
            jobs_tbl,
            country=country,
            status=status_filter.value if status_filter else None,
            triggered_by=None if user.is_admin else user.email,
            limit=limit,
            offset=offset,
        )
        for row in delta_jobs:
            if row["job_id"] in sqlite_job_ids:
                continue
            failed_queries = row.get("failed_queries", [])
            items.append(
                EventSummary(
                    job_id=row["job_id"],
                    status=row["status"],
                    country=row["country"],
                    stage=row["stage"],
                    tag=row["tag"],
                    queries_total=len(row["queries"]),
                    queries_completed=0,
                    queries_failed=len(failed_queries),
                    created_at=row["created_at"],
                    started_at=row.get("started_at"),
                    completed_at=row.get("completed_at"),
                    triggered_by=row["triggered_by"],
                    error=row.get("error"),
                    current_query=None,
                    failed_queries=failed_queries,
                )
            )
            total += 1
    except Exception:
        logger.debug("Delta table fallback for list_events failed (non-critical)")

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
    db_path = _get_db_path(raw_request)

    # Try SQLite first (primary store with live progress)
    row = local_store.get_job(db_path, job_id)

    if row:
        # Non-admin users can only see their own jobs
        if not user.is_admin and row["triggered_by"] != user.email:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error": "not_found", "message": f"Job not found: {job_id}"},
            )

        results = _results_from_row(row)
        q_completed, q_failed = _compute_counts(results)
        running_queries = row.get("running_queries", [])

        return EventDetail(
            job_id=row["job_id"],
            status=row["status"],
            country=row["country"],
            stage=row["stage"],
            tag=row["tag"],
            queries_total=len(row["queries"]),
            queries_completed=q_completed,
            queries_failed=q_failed,
            created_at=row["created_at"],
            started_at=row.get("started_at"),
            completed_at=row.get("completed_at"),
            triggered_by=row["triggered_by"],
            error=row.get("error"),
            current_query=row.get("current_query"),
            results=results,
            failed_queries=row.get("failed_queries", []),
            running_queries=running_queries,
            queries_running=len(running_queries),
            total_rows_extracted=_total_rows(results),
        )

    # Fallback to Delta table for pre-SQLite jobs
    try:
        db_client = _get_databricks_client(raw_request)
        jobs_tbl = get_settings().jobs_table
        db_row = delta_get_job(db_client, jobs_tbl, job_id)
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

    failed_queries = db_row.get("failed_queries", [])
    return EventDetail(
        job_id=db_row["job_id"],
        status=db_row["status"],
        country=db_row["country"],
        stage=db_row["stage"],
        tag=db_row["tag"],
        queries_total=len(db_row["queries"]),
        queries_completed=0,
        queries_failed=len(failed_queries),
        created_at=db_row["created_at"],
        started_at=db_row.get("started_at"),
        completed_at=db_row.get("completed_at"),
        triggered_by=db_row["triggered_by"],
        error=db_row.get("error"),
        results=[],
        failed_queries=failed_queries,
    )
