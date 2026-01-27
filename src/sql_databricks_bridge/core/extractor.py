"""SQL data extraction module."""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterator
from uuid import uuid4

import polars as pl

from sql_databricks_bridge.api.schemas import JobStatus, QueryResult
from sql_databricks_bridge.core.param_resolver import ParamResolver
from sql_databricks_bridge.core.query_loader import QueryLoader
from sql_databricks_bridge.db.sql_server import SQLServerClient

logger = logging.getLogger(__name__)


@dataclass
class ExtractionJob:
    """Represents an extraction job."""

    job_id: str
    country: str
    destination: str = ""
    queries: list[str] = field(default_factory=list)
    chunk_size: int = 100_000
    status: JobStatus = JobStatus.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    results: list[QueryResult] = field(default_factory=list)
    error: str | None = None

    @property
    def queries_completed(self) -> int:
        """Count of completed queries."""
        return sum(1 for r in self.results if r.status == JobStatus.COMPLETED)

    @property
    def queries_failed(self) -> int:
        """Count of failed queries."""
        return sum(1 for r in self.results if r.status == JobStatus.FAILED)


class Extractor:
    """Extracts data from SQL Server using configured queries."""

    def __init__(
        self,
        queries_path: str,
        config_path: str,
        sql_client: SQLServerClient | None = None,
    ) -> None:
        """Initialize extractor.

        Args:
            queries_path: Path to SQL query files.
            config_path: Path to YAML config files.
            sql_client: SQL Server client (creates new one if not provided).
        """
        self.query_loader = QueryLoader(queries_path)
        self.param_resolver = ParamResolver(config_path)
        self.sql_client = sql_client or SQLServerClient()
        self._jobs: dict[str, ExtractionJob] = {}

    def create_job(
        self,
        country: str,
        destination: str,
        queries: list[str] | None = None,
        chunk_size: int = 100_000,
    ) -> ExtractionJob:
        """Create a new extraction job.

        Args:
            country: Country name for parameter resolution.
            destination: Databricks volume path for output.
            queries: Specific queries to run (None = all).
            chunk_size: Rows per extraction chunk.

        Returns:
            Created extraction job.
        """
        job_id = str(uuid4())

        # Get available queries
        available_queries = self.query_loader.list_queries()

        if queries:
            # Validate requested queries exist
            missing = [q for q in queries if q not in available_queries]
            if missing:
                raise ValueError(f"Unknown queries: {missing}")
            selected_queries = queries
        else:
            selected_queries = available_queries

        job = ExtractionJob(
            job_id=job_id,
            country=country,
            destination=destination,
            queries=selected_queries,
            chunk_size=chunk_size,
        )

        self._jobs[job_id] = job
        logger.info(f"Created extraction job {job_id} with {len(selected_queries)} queries")

        return job

    def get_job(self, job_id: str) -> ExtractionJob | None:
        """Get job by ID."""
        return self._jobs.get(job_id)

    def execute_query(
        self,
        query_name: str,
        country: str,
        chunk_size: int = 100_000,
        limit: int | None = None,
        extra_params: dict[str, str] | None = None,
    ) -> Iterator[pl.DataFrame]:
        """Execute a single query and yield result chunks.

        Args:
            query_name: Name of query to execute.
            country: Country for parameter resolution.
            chunk_size: Rows per chunk.
            limit: Optional row limit (for testing). Wraps query with SELECT TOP N.
            extra_params: Additional parameters to merge (override YAML params).

        Yields:
            DataFrame chunks with query results.
        """
        # Resolve parameters from YAML
        params = self.param_resolver.resolve_params(country)

        # Merge extra parameters (override YAML values)
        if extra_params:
            params.update(extra_params)

        # Format query with parameters
        formatted_query = self.query_loader.format_query(query_name, params)

        # Apply row limit if specified (SQL Server TOP syntax)
        if limit is not None and limit > 0:
            formatted_query = f"SELECT TOP {limit} * FROM ({formatted_query}) AS _limited_subquery"
            logger.info(f"Executing query: {query_name} (limited to {limit} rows)")
        else:
            logger.info(f"Executing query: {query_name}")

        logger.debug(f"Formatted query: {formatted_query[:200]}...")

        # Execute and yield chunks
        yield from self.sql_client.execute_query_chunked(
            formatted_query,
            chunk_size=chunk_size,
        )

    def run_extraction(
        self,
        job: ExtractionJob,
    ) -> ExtractionJob:
        """Run extraction for all queries in a job.

        Args:
            job: Extraction job to run.

        Returns:
            Updated job with results.
        """
        job.status = JobStatus.RUNNING
        job.started_at = datetime.utcnow()

        logger.info(f"Starting extraction job {job.job_id}")

        for query_name in job.queries:
            result = QueryResult(
                query_name=query_name,
                status=JobStatus.RUNNING,
            )

            start_time = datetime.utcnow()

            try:
                chunks: list[pl.DataFrame] = []
                total_rows = 0

                for chunk in self.execute_query(
                    query_name,
                    job.country,
                    job.chunk_size,
                ):
                    chunks.append(chunk)
                    total_rows += len(chunk)

                # Combine all chunks
                if chunks:
                    combined = pl.concat(chunks)
                    result.rows_extracted = len(combined)
                else:
                    combined = pl.DataFrame()
                    result.rows_extracted = 0

                result.status = JobStatus.COMPLETED
                logger.info(f"Query {query_name}: extracted {result.rows_extracted} rows")

            except Exception as e:
                result.status = JobStatus.FAILED
                result.error = str(e)
                logger.error(f"Query {query_name} failed: {e}")

            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            job.results.append(result)

        # Update job status
        if job.queries_failed > 0:
            job.status = JobStatus.FAILED if job.queries_completed == 0 else JobStatus.COMPLETED
        else:
            job.status = JobStatus.COMPLETED

        job.completed_at = datetime.utcnow()

        logger.info(
            f"Job {job.job_id} completed: "
            f"{job.queries_completed}/{len(job.queries)} succeeded"
        )

        return job
