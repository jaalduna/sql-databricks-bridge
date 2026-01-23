"""SQL Server database connection and operations."""

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from sql_databricks_bridge.core.config import SQLServerSettings, get_settings


class SQLServerClient:
    """SQL Server database client."""

    def __init__(self, settings: SQLServerSettings | None = None) -> None:
        """Initialize SQL Server client.

        Args:
            settings: SQL Server settings. Uses default settings if not provided.
        """
        self.settings = settings or get_settings().sql_server
        self._engine: Engine | None = None

    @property
    def engine(self) -> Engine:
        """Get or create SQLAlchemy engine."""
        if self._engine is None:
            self._engine = create_engine(
                self.settings.sqlalchemy_url,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
            )
        return self._engine

    def test_connection(self) -> bool:
        """Test database connectivity.

        Returns:
            True if connection successful, False otherwise.
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError:
            return False

    def execute_query(self, query: str, params: dict[str, Any] | None = None) -> pl.DataFrame:
        """Execute query and return results as Polars DataFrame.

        Args:
            query: SQL query to execute.
            params: Query parameters.

        Returns:
            Query results as Polars DataFrame.
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            columns = list(result.keys())
            rows = result.fetchall()

        if not rows:
            return pl.DataFrame(schema={col: pl.Utf8 for col in columns})

        return pl.DataFrame([dict(zip(columns, row)) for row in rows])

    def execute_query_chunked(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        chunk_size: int = 100_000,
    ) -> AsyncIterator[pl.DataFrame]:
        """Execute query and yield results in chunks.

        Args:
            query: SQL query to execute.
            params: Query parameters.
            chunk_size: Number of rows per chunk.

        Yields:
            Polars DataFrames with chunk_size rows each.
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            columns = list(result.keys())

            while True:
                rows = result.fetchmany(chunk_size)
                if not rows:
                    break

                yield pl.DataFrame([dict(zip(columns, row)) for row in rows])

    def execute_write(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        """Execute write operation (INSERT/UPDATE/DELETE).

        Args:
            query: SQL query to execute.
            params: Query parameters.

        Returns:
            Number of affected rows.
        """
        with self.engine.begin() as conn:
            result = conn.execute(text(query), params or {})
            return result.rowcount

    def bulk_insert(
        self,
        table: str,
        df: pl.DataFrame,
        schema: str = "dbo",
    ) -> int:
        """Bulk insert DataFrame into table.

        Args:
            table: Target table name.
            df: Data to insert.
            schema: Schema name.

        Returns:
            Number of inserted rows.
        """
        if df.is_empty():
            return 0

        columns = df.columns
        placeholders = ", ".join([f":{col}" for col in columns])
        column_list = ", ".join([f"[{col}]" for col in columns])

        query = f"INSERT INTO [{schema}].[{table}] ({column_list}) VALUES ({placeholders})"

        records = df.to_dicts()
        with self.engine.begin() as conn:
            conn.execute(text(query), records)

        return len(records)

    def close(self) -> None:
        """Close database connection."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None


@asynccontextmanager
async def get_sql_client() -> AsyncIterator[SQLServerClient]:
    """Async context manager for SQL Server client."""
    client = SQLServerClient()
    try:
        yield client
    finally:
        client.close()
