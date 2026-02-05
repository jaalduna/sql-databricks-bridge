"""Databricks connection and file operations."""

from contextlib import asynccontextmanager
from io import BytesIO
from typing import Any, AsyncIterator

import polars as pl
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.files import FileInfo

from sql_databricks_bridge.core.config import DatabricksSettings, get_settings


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
        # Use the SQL warehouse for query execution
        # This requires a warehouse_id to be configured
        statement = self.client.statement_execution.execute_statement(
            warehouse_id=getattr(self.settings, "warehouse_id", ""),
            statement=query,
            wait_timeout="30s",
        )

        if statement.result and statement.result.data_array:
            columns = [col.name for col in (statement.manifest.schema.columns or [])]
            return [dict(zip(columns, row)) for row in statement.result.data_array]

        return []


@asynccontextmanager
async def get_databricks_client() -> AsyncIterator[DatabricksClient]:
    """Async context manager for Databricks client."""
    client = DatabricksClient()
    try:
        yield client
    finally:
        pass  # WorkspaceClient doesn't need explicit cleanup
