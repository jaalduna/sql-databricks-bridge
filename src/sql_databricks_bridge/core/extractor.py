"""SQL data extraction module."""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterator
from uuid import uuid4

from dateutil.relativedelta import relativedelta

import polars as pl

from sql_databricks_bridge.api.schemas import JobStatus, QueryResult
from sql_databricks_bridge.core.country_query_loader import CountryAwareQueryLoader
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
    current_query: str | None = None

    @property
    def queries_completed(self) -> int:
        """Count of completed queries."""
        return sum(1 for r in self.results if r.status == JobStatus.COMPLETED)

    @property
    def queries_failed(self) -> int:
        """Count of failed queries."""
        return sum(1 for r in self.results if r.status == JobStatus.FAILED)


class Extractor:
    """Extracts data from SQL Server using country-aware queries."""

    def __init__(
        self,
        queries_path: str,
        sql_client: SQLServerClient | None = None,
    ) -> None:
        """Initialize extractor.

        Args:
            queries_path: Root path containing 'common/' and 'countries/' query directories.
            sql_client: SQL Server client (creates new one if not provided).
        """
        self.query_loader = CountryAwareQueryLoader(queries_path)
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
            country: Country name for query discovery.
            destination: Databricks destination (e.g., catalog.schema).
            queries: Specific queries to run (None = all available for country).
            chunk_size: Rows per extraction chunk.

        Returns:
            Created extraction job.
        """
        job_id = str(uuid4())

        # Get available queries for this country
        available_queries = self.query_loader.list_queries(country)

        if queries:
            # Validate requested queries exist for this country
            missing = [q for q in queries if q not in available_queries]
            if missing:
                raise ValueError(
                    f"Queries not available for {country}: {missing}. "
                    f"Available: {available_queries}"
                )
            selected_queries = queries
        else:
            # Use all available queries for this country
            selected_queries = available_queries

        job = ExtractionJob(
            job_id=job_id,
            country=country,
            destination=destination,
            queries=selected_queries,
            chunk_size=chunk_size,
        )

        self._jobs[job_id] = job
        logger.info(
            f"Created extraction job {job_id} for {country} "
            f"with {len(selected_queries)} queries"
        )

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
        lookback_months: int = 24,
    ) -> Iterator[pl.DataFrame]:
        """Execute a single query and yield result chunks.

        Args:
            query_name: Name of query to execute.
            country: Country for query resolution.
            chunk_size: Rows per chunk.
            limit: Optional row limit (for testing). Wraps query with SELECT TOP N.
            lookback_months: Number of months to look back for fact queries (default: 24).

        Yields:
            DataFrame chunks with query results.
        """
        # Get pre-resolved query for this country
        query_sql = self.query_loader.get_query(query_name, country)

        # Substitute time-filter placeholders based on lookback_months
        query_sql = query_sql.replace("{lookback_months}", str(lookback_months))

        now = datetime.utcnow()
        start = now - relativedelta(months=lookback_months)
        query_sql = query_sql.replace("{start_period}", start.strftime("%Y%m"))
        query_sql = query_sql.replace("{end_period}", now.strftime("%Y%m"))
        query_sql = query_sql.replace("{start_year}", str(start.year))
        query_sql = query_sql.replace("{end_year}", str(now.year))
        query_sql = query_sql.replace("{start_date}", f"'{start.strftime('%Y-%m-01')}'")
        query_sql = query_sql.replace("{end_date}", f"'{now.strftime('%Y-%m-%d')}'")

        # Apply row limit if specified (SQL Server TOP syntax)
        if limit is not None and limit > 0:
            query_sql = f"SELECT TOP {limit} * FROM ({query_sql}) AS _limited_subquery"
            logger.info(f"Executing query: {query_name} for {country} (limited to {limit} rows)")
        else:
            logger.info(f"Executing query: {query_name} for {country}")

        logger.debug(f"Query SQL: {query_sql[:200]}...")

        # Execute and yield chunks
        yield from self.sql_client.execute_query_chunked(
            query_sql,
            chunk_size=chunk_size,
        )

    def run_extraction(
        self,
        job: ExtractionJob,
        lookback_months: int = 24,
    ) -> ExtractionJob:
        """Run extraction for all queries in a job.

        Args:
            job: Extraction job to run.
            lookback_months: Number of months to look back for fact queries.

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
                    lookback_months=lookback_months,
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
