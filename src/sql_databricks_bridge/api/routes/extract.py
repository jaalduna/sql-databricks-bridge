"""Extraction API endpoints."""

import logging
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status

from sql_databricks_bridge.api.schemas import (
    ExtractionRequest,
    ExtractionResponse,
    JobStatus,
)
from sql_databricks_bridge.core.delta_writer import DeltaTableWriter
from sql_databricks_bridge.core.extractor import Extractor, ExtractionJob
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.sql_server import SQLServerClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/extract", tags=["Extraction"])

# In-memory job storage (would use Redis or DB in production)
_extractors: dict[str, Extractor] = {}


def get_extractor(request: ExtractionRequest) -> Extractor:
    """Create extractor for request with country-specific SQL Server connection."""
    sql_client = SQLServerClient(country=request.country)
    return Extractor(
        queries_path=request.queries_path,
        config_path=request.config_path,
        sql_client=sql_client,
    )


async def run_extraction_job(
    extractor: Extractor,
    job: ExtractionJob,
    writer: DeltaTableWriter,
    overwrite: bool,
    target_catalog: str | None = None,
    target_schema: str | None = None,
) -> None:
    """Background task to run extraction."""
    try:
        job.status = JobStatus.RUNNING

        for query_name in job.queries:
            table_name = writer.resolve_table_name(
                query_name, job.country, target_catalog, target_schema
            )

            if not overwrite and writer.table_exists(table_name):
                logger.info(f"Skipping {query_name} - table {table_name} exists and overwrite=False")
                continue

            # Execute query and write to Delta table
            chunks = list(
                extractor.execute_query(
                    query_name,
                    job.country,
                    job.chunk_size,
                )
            )

            if chunks:
                import polars as pl

                combined = pl.concat(chunks)
                writer.write_dataframe(
                    combined,
                    query_name,
                    job.country,
                    catalog=target_catalog,
                    schema=target_schema,
                )

        job.status = JobStatus.COMPLETED

    except Exception as e:
        job.status = JobStatus.FAILED
        job.error = str(e)
        logger.error(f"Extraction job {job.job_id} failed: {e}")


@router.post(
    "",
    response_model=ExtractionResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Start extraction job",
    description="Start a data extraction job from SQL Server to Databricks.",
)
async def start_extraction(
    request: ExtractionRequest,
    background_tasks: BackgroundTasks,
) -> ExtractionResponse:
    """Start a new extraction job."""
    try:
        extractor = get_extractor(request)

        job = extractor.create_job(
            country=request.country,
            destination=request.destination or "",
            queries=request.queries,
            chunk_size=request.chunk_size,
        )

        # Store extractor for job lookup
        _extractors[job.job_id] = extractor

        # Create Delta table writer
        writer = DeltaTableWriter(DatabricksClient())

        # Run extraction in background
        background_tasks.add_task(
            run_extraction_job,
            extractor,
            job,
            writer,
            request.overwrite,
            request.target_catalog,
            request.target_schema,
        )

        return ExtractionResponse(
            job_id=job.job_id,
            status=JobStatus.PENDING,
            message="Extraction job started",
            queries_count=len(job.queries),
        )

    except FileNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        logger.error(f"Failed to start extraction: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to start extraction job",
        )
