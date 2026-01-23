"""Databricks file upload module."""

import logging
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Iterator

import polars as pl

from sql_databricks_bridge.db.databricks import DatabricksClient

logger = logging.getLogger(__name__)


@dataclass
class UploadResult:
    """Result of a file upload."""

    path: str
    rows: int
    size_bytes: int
    duration_seconds: float


class Uploader:
    """Uploads data to Databricks Volumes."""

    def __init__(self, client: DatabricksClient | None = None) -> None:
        """Initialize uploader.

        Args:
            client: Databricks client (creates new one if not provided).
        """
        self.client = client or DatabricksClient()

    def upload_dataframe(
        self,
        df: pl.DataFrame,
        path: str,
        compression: str = "zstd",
        overwrite: bool = True,
    ) -> UploadResult:
        """Upload DataFrame as single Parquet file.

        Args:
            df: DataFrame to upload.
            path: Full path in Databricks Volume.
            compression: Parquet compression (zstd, snappy, gzip).
            overwrite: Overwrite existing file.

        Returns:
            Upload result with metadata.
        """
        start_time = datetime.utcnow()

        buffer = BytesIO()
        df.write_parquet(buffer, compression=compression)
        size_bytes = buffer.tell()
        buffer.seek(0)

        self.client.client.files.upload(
            file_path=path,
            contents=buffer,
            overwrite=overwrite,
        )

        duration = (datetime.utcnow() - start_time).total_seconds()

        logger.info(f"Uploaded {len(df)} rows ({size_bytes / 1024 / 1024:.2f} MB) to {path}")

        return UploadResult(
            path=path,
            rows=len(df),
            size_bytes=size_bytes,
            duration_seconds=duration,
        )

    def upload_chunks(
        self,
        chunks: Iterator[pl.DataFrame],
        base_path: str,
        compression: str = "zstd",
    ) -> list[UploadResult]:
        """Upload DataFrame chunks as multiple Parquet files.

        Args:
            chunks: Iterator of DataFrames to upload.
            base_path: Base directory path (files named part_00000.parquet, etc).
            compression: Parquet compression.

        Returns:
            List of upload results.
        """
        results = []

        for i, chunk in enumerate(chunks):
            chunk_path = f"{base_path}/part_{i:05d}.parquet"
            result = self.upload_dataframe(
                chunk,
                chunk_path,
                compression=compression,
            )
            results.append(result)

        total_rows = sum(r.rows for r in results)
        total_bytes = sum(r.size_bytes for r in results)

        logger.info(
            f"Uploaded {len(results)} chunks, "
            f"{total_rows} rows ({total_bytes / 1024 / 1024:.2f} MB total)"
        )

        return results

    def upload_query_result(
        self,
        df: pl.DataFrame,
        destination: str,
        query_name: str,
        country: str,
        chunk_size: int = 100_000,
        compression: str = "zstd",
    ) -> list[UploadResult]:
        """Upload query result with standard naming.

        Creates files at: {destination}/{country}/{query_name}/part_XXXXX.parquet

        Args:
            df: DataFrame with query results.
            destination: Base destination path.
            query_name: Name of the query.
            country: Country name.
            chunk_size: Rows per chunk (0 = single file).
            compression: Parquet compression.

        Returns:
            List of upload results.
        """
        base_path = f"{destination}/{country}/{query_name}"

        if chunk_size == 0 or len(df) <= chunk_size:
            # Single file
            path = f"{base_path}/{query_name}.parquet"
            result = self.upload_dataframe(df, path, compression=compression)
            return [result]

        # Multiple chunks
        def chunk_generator() -> Iterator[pl.DataFrame]:
            for i in range(0, len(df), chunk_size):
                yield df.slice(i, chunk_size)

        return self.upload_chunks(chunk_generator(), base_path, compression)

    def file_exists(self, path: str) -> bool:
        """Check if file exists in Databricks Volume.

        Args:
            path: File path to check.

        Returns:
            True if file exists.
        """
        try:
            self.client.client.files.get_status(path)
            return True
        except Exception:
            return False

    def delete_path(self, path: str, recursive: bool = False) -> None:
        """Delete file or directory.

        Args:
            path: Path to delete.
            recursive: Delete directory recursively.
        """
        try:
            if recursive:
                # List and delete all files
                for file_info in self.client.list_files(path):
                    self.client.delete_file(file_info.path)
            self.client.delete_file(path)
            logger.info(f"Deleted {path}")
        except Exception as e:
            logger.warning(f"Failed to delete {path}: {e}")
