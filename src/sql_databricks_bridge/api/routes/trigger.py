"""Trigger API endpoints -- frontend-facing sync trigger and event views."""

import logging
import os
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from sql_databricks_bridge.api.dependencies import CurrentAzureADUser
from sql_databricks_bridge.api.schemas import JobStatus, QueryResult
from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.core.delta_writer import DeltaTableWriter
from sql_databricks_bridge.core.extractor import Extractor, ExtractionJob
from sql_databricks_bridge.core.stages import build_tag
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.jobs_table import (
    get_job,
    insert_job,
    list_jobs,
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
    ) -> None:
        self.extractor = extractor
        self.job = job
        self.triggered_by = triggered_by
        self.stage = stage
        self.tag = tag


# Fix forward reference for module-level dict
_trigger_jobs: dict[str, _TriggerJobRecord] = {}


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
) -> None:
    """Background task to run the extraction triggered by a user."""
    try:
        import polars as pl

        job.status = JobStatus.RUNNING
        job.started_at = datetime.utcnow()
        update_job_status(client, table, job.job_id, "running", started_at=job.started_at)

        for query_name in job.queries:
            job.current_query = query_name
            result = QueryResult(query_name=query_name, status=JobStatus.RUNNING)
            start_time = datetime.utcnow()

            try:
                row_limit = int(os.environ.get("QUERY_ROW_LIMIT", "0")) or None
                chunks = list(
                    extractor.execute_query(query_name, job.country, job.chunk_size, limit=row_limit)
                )

                if chunks:
                    combined = pl.concat(chunks)
                    writer.write_dataframe(combined, query_name, job.country)
                    result.rows_extracted = len(combined)
                    result.table_name = writer.resolve_table_name(
                        query_name, job.country
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
            job.results.append(result)

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

    except Exception as e:
        job.current_query = None
        job.status = JobStatus.FAILED
        job.error = str(e)
        logger.error(f"Trigger job {job.job_id} failed: {e}")
        update_job_status(client, table, job.job_id, "failed", error=str(e))


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

    # Persist to Databricks Delta table
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
    )

    # Store in-flight record (needed for background task reference)
    record = _TriggerJobRecord(
        extractor=extractor,
        job=job,
        triggered_by=user.email,
        stage=request.stage,
        tag=tag,
    )
    _trigger_jobs[job.job_id] = record

    # Launch background extraction
    writer = DeltaTableWriter(db_client)
    background_tasks.add_task(_run_trigger_extraction, extractor, job, writer, db_client, jobs_tbl)

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
    db_client = _get_databricks_client(raw_request)
    jobs_tbl = get_settings().jobs_table

    db_jobs, total = list_jobs(
        db_client,
        jobs_tbl,
        country=country,
        status=status_filter.value if status_filter else None,
        triggered_by=None if user.is_admin else user.email,
        limit=limit,
        offset=offset,
    )

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
        else:
            queries_completed = 0
            queries_failed = 0
            live_status = row["status"]
            started_at = row.get("started_at")
            completed_at = row.get("completed_at")
            error = row.get("error")
            current_query = None

        items.append(
            EventSummary(
                job_id=row["job_id"],
                status=live_status,
                country=row["country"],
                stage=row["stage"],
                tag=row["tag"],
                queries_total=len(row["queries"]),
                queries_completed=queries_completed,
                queries_failed=queries_failed,
                created_at=row["created_at"],
                started_at=started_at,
                completed_at=completed_at,
                triggered_by=row["triggered_by"],
                error=error,
                current_query=current_query,
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
        if not user.is_admin and record.triggered_by != user.email:
            continue
        items.insert(
            0,
            EventSummary(
                job_id=job.job_id,
                status=job.status,
                country=job.country,
                stage=record.stage,
                tag=record.tag,
                queries_total=len(job.queries),
                queries_completed=job.queries_completed,
                queries_failed=job.queries_failed,
                created_at=job.created_at,
                started_at=job.started_at,
                completed_at=job.completed_at,
                triggered_by=record.triggered_by,
                error=job.error,
                current_query=job.current_query,
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

    # Check in-flight record first (avoids Delta table read-after-write lag)
    record = _trigger_jobs.get(job_id)

    db_row = get_job(db_client, jobs_tbl, job_id)

    if not db_row and not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Job not found: {job_id}"},
        )

    # Non-admin users can only see their own jobs
    triggered_by = record.triggered_by if record else db_row["triggered_by"]
    if not user.is_admin and triggered_by != user.email:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Job not found: {job_id}"},
        )

    # If there's an in-flight record, use live progress
    if record:
        job = record.job
        # Prefer db_row for stage/tag if available, fallback to in-flight data
        stage = db_row["stage"] if db_row else record.stage
        tag = db_row["tag"] if db_row else record.tag
        return EventDetail(
            job_id=job.job_id,
            status=job.status,
            country=job.country,
            stage=stage,
            tag=tag,
            queries_total=len(job.queries),
            queries_completed=job.queries_completed,
            queries_failed=job.queries_failed,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            triggered_by=record.triggered_by,
            error=job.error,
            current_query=job.current_query,
            results=job.results,
        )

    # Job completed and no longer in-flight -- return from Databricks
    return EventDetail(
        job_id=db_row["job_id"],
        status=db_row["status"],
        country=db_row["country"],
        stage=db_row["stage"],
        tag=db_row["tag"],
        queries_total=len(db_row["queries"]),
        queries_completed=0,
        queries_failed=0,
        created_at=db_row["created_at"],
        started_at=db_row.get("started_at"),
        completed_at=db_row.get("completed_at"),
        triggered_by=db_row["triggered_by"],
        error=db_row.get("error"),
        results=[],
    )
