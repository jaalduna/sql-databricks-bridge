"""Databricks connection and file operations."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from io import BytesIO
from typing import Any, AsyncIterator

import polars as pl
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.files import FileInfo

from sql_databricks_bridge.core.config import DatabricksSettings, get_settings

logger = logging.getLogger(__name__)


class DatabricksClient:
    """Databricks client for file and table operations."""

    def __init__(self, settings: DatabricksSettings | None = None) -> None:
        """Initialize Databricks client.

        Args:
            settings: Databricks settings. Uses default settings if not provided.
        """
        self.settings = settings or get_settings().databricks
        self._client: WorkspaceClient | None = None

    @property
    def client(self) -> WorkspaceClient:
        """Get or create WorkspaceClient."""
        if self._client is None:
            if self.settings.client_id and self.settings.client_secret:
                # Service Principal authentication
                self._client = WorkspaceClient(
                    host=self.settings.host,
                    client_id=self.settings.client_id,
                    client_secret=self.settings.client_secret.get_secret_value()
                    if self.settings.client_secret
                    else None,
                )
            elif self.settings.host and self.settings.token.get_secret_value():
                # Token authentication (explicit)
                self._client = WorkspaceClient(
                    host=self.settings.host,
                    token=self.settings.token.get_secret_value(),
                )
            else:
                # Default Databricks SDK authentication chain (env / ~/.databrickscfg / etc.)
                # This avoids requiring DATABRICKS_HOST/TOKEN to be set in the shell.
                self._client = WorkspaceClient()
        return self._client

    def test_connection(self) -> bool:
        """Test Databricks connectivity.

        Returns:
            True if connection successful, False otherwise.
        """
        try:
            self.client.current_user.me()
            return True
        except Exception:
            return False

    def upload_dataframe(
        self,
        df: pl.DataFrame,
        path: str,
        overwrite: bool = True,
        compression: str = "zstd",
    ) -> str:
        """Upload DataFrame as Parquet to Databricks Volume.

        Args:
            df: DataFrame to upload.
            path: Full path in Databricks Volume.
            overwrite: Overwrite existing file.
            compression: Parquet compression (zstd, snappy, gzip).

        Returns:
            Uploaded file path.
        """
        buffer = BytesIO()
        df.write_parquet(buffer, compression=compression)
        buffer.seek(0)

        self.client.files.upload(
            file_path=path,
            contents=buffer,
            overwrite=overwrite,
        )

        return path

    def upload_dataframe_chunked(
        self,
        chunks: list[pl.DataFrame],
        base_path: str,
        compression: str = "zstd",
    ) -> list[str]:
        """Upload DataFrame chunks as multiple Parquet files.

        Args:
            chunks: List of DataFrames to upload.
            base_path: Base path without extension.
            compression: Parquet compression.

        Returns:
            List of uploaded file paths.
        """
        paths = []
        for i, chunk in enumerate(chunks):
            chunk_path = f"{base_path}/part_{i:05d}.parquet"
            self.upload_dataframe(chunk, chunk_path, compression=compression)
            paths.append(chunk_path)
        return paths

    def list_files(self, path: str) -> list[FileInfo]:
        """List files in a directory.

        Args:
            path: Directory path.

        Returns:
            List of file information.
        """
        return list(self.client.files.list_directory_contents(path))

    def download_dataframe(self, path: str) -> pl.DataFrame:
        """Download Parquet file as DataFrame.

        Args:
            path: File path in Databricks Volume.

        Returns:
            DataFrame with file contents.
        """
        response = self.client.files.download(path)
        buffer = BytesIO(response.contents.read())
        return pl.read_parquet(buffer)

    def delete_file(self, path: str) -> None:
        """Delete file from Databricks Volume.

        Args:
            path: File path to delete.
        """
        self.client.files.delete(path)

    def execute_sql(self, query: str) -> list[dict[str, Any]]:
        """Execute SQL query using Statement Execution API.

        Args:
            query: SQL query to execute.

        Returns:
            Query results as list of dictionaries.
        """
        logger.debug("Executing SQL query: %s", query)

        # Use the SQL warehouse for query execution
        # This requires a warehouse_id to be configured
        try:
            statement = self.client.statement_execution.execute_statement(
                warehouse_id=getattr(self.settings, "warehouse_id", ""),
                statement=query,
                wait_timeout="30s",
            )
        except Exception:
            logger.exception("execute_statement() failed for query: %s", query)
            raise

        logger.debug("Statement status: %s", statement.status)

        if statement.result is None:
            logger.warning(
                "statement.result is None — status: %s, error: %s, query: %s",
                statement.status,
                getattr(statement.status, "error", None) if statement.status else None,
                query,
            )
            return []

        if not statement.result.data_array:
            logger.debug("Query returned no rows (data_array is empty/None)")
            return []

        columns = [col.name for col in (statement.manifest.schema.columns or [])]
        rows = [dict(zip(columns, row)) for row in statement.result.data_array]
        logger.debug("Query returned %d row(s)", len(rows))
        return rows

    # --- Jobs API ---

    def find_job_by_name(self, name: str) -> int | None:
        """Find a Databricks job ID by name (substring match).

        First tries an exact-name API filter. If that returns nothing, falls
        back to iterating all jobs and checking for a substring match (needed
        when DAB-deployed jobs have prefixes like ``[dev] Bronze Copy``).
        """
        # Fast path: exact name match via API filter
        for job in self.client.jobs.list(name=name):
            if job.settings and job.settings.name and name in job.settings.name:
                return job.job_id

        # Slow path: iterate all jobs with substring match
        for job in self.client.jobs.list():
            if job.settings and job.settings.name and name in job.settings.name:
                logger.info("Found job '%s' via substring match (id=%d)", job.settings.name, job.job_id)
                return job.job_id

        return None

    def run_job(self, job_id: int, parameters: dict[str, str] | None = None) -> int:
        """Trigger an existing Databricks job and return the run ID.

        Args:
            job_id: Numeric Databricks job ID.
            parameters: Job parameters dict (maps to ``job_parameters``).

        Returns:
            The ``run_id`` of the triggered run.
        """
        response = self.client.jobs.run_now(
            job_id=job_id,
            job_parameters=parameters or {},
        )
        logger.info("Triggered Databricks job %d → run_id=%d", job_id, response.run_id)
        return response.run_id

    def get_run(self, run_id: int):
        """Return the Databricks run object for *run_id*."""
        return self.client.jobs.get_run(run_id)


@asynccontextmanager
async def get_databricks_client() -> AsyncIterator[DatabricksClient]:
    """Async context manager for Databricks client."""
    client = DatabricksClient()
    try:
        yield client
    finally:
        pass  # WorkspaceClient doesn't need explicit cleanup
