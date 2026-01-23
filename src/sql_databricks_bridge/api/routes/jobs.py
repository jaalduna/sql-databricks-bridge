"""Job status API endpoints."""

import logging

from fastapi import APIRouter, HTTPException, status

from sql_databricks_bridge.api.routes.extract import _extractors
from sql_databricks_bridge.api.schemas import JobStatus, JobStatusResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/jobs", tags=["Jobs"])


@router.get(
    "/{job_id}",
    response_model=JobStatusResponse,
    summary="Get job status",
    description="Get the status of an extraction job.",
)
async def get_job_status(job_id: str) -> JobStatusResponse:
    """Get status of an extraction job."""
    # Find the extractor that has this job
    extractor = _extractors.get(job_id)

    if not extractor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job not found: {job_id}",
        )

    job = extractor.get_job(job_id)

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job not found: {job_id}",
        )

    return JobStatusResponse(
        job_id=job.job_id,
        status=job.status,
        country=job.country,
        destination=job.destination,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        queries_total=len(job.queries),
        queries_completed=job.queries_completed,
        queries_failed=job.queries_failed,
        results=job.results,
        error=job.error,
    )


@router.delete(
    "/{job_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Cancel job",
    description="Cancel a running extraction job.",
)
async def cancel_job(job_id: str) -> None:
    """Cancel a running extraction job."""
    extractor = _extractors.get(job_id)

    if not extractor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job not found: {job_id}",
        )

    job = extractor.get_job(job_id)

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job not found: {job_id}",
        )

    if job.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot cancel job with status: {job.status}",
        )

    job.status = JobStatus.CANCELLED
    logger.info(f"Cancelled job {job_id}")


@router.get(
    "",
    response_model=list[JobStatusResponse],
    summary="List jobs",
    description="List all extraction jobs.",
)
async def list_jobs(
    status_filter: JobStatus | None = None,
    limit: int = 100,
) -> list[JobStatusResponse]:
    """List all extraction jobs."""
    jobs = []

    for job_id, extractor in _extractors.items():
        job = extractor.get_job(job_id)
        if job:
            if status_filter and job.status != status_filter:
                continue

            jobs.append(
                JobStatusResponse(
                    job_id=job.job_id,
                    status=job.status,
                    country=job.country,
                    destination=job.destination,
                    created_at=job.created_at,
                    started_at=job.started_at,
                    completed_at=job.completed_at,
                    queries_total=len(job.queries),
                    queries_completed=job.queries_completed,
                    queries_failed=job.queries_failed,
                    results=job.results,
                    error=job.error,
                )
            )

    # Sort by creation time (newest first) and limit
    jobs.sort(key=lambda j: j.created_at, reverse=True)
    return jobs[:limit]
