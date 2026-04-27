"""SQL Server database connection and operations."""

import datetime
import decimal
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Callable

import polars as pl
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from sql_databricks_bridge.core.config import SQLServerSettings, get_settings

logger = logging.getLogger(__name__)

# Mapping from pyodbc cursor.description type_code (Python types) to Polars types.
# pyodbc exposes the column type as a Python type object in cursor.description[i][1].
_PYODBC_TYPE_MAP: dict[type, pl.DataType] = {
    str: pl.String,
    int: pl.Int64,
    float: pl.Float64,
    decimal.Decimal: pl.Float64,
    datetime.datetime: pl.Datetime,
    datetime.date: pl.Date,
    bytes: pl.Binary,
    bool: pl.Boolean,
}


def _schema_from_cursor(description: list[tuple]) -> dict[str, pl.DataType]:
    """Extract column name -> Polars dtype mapping from pyodbc cursor.description.

    Args:
        description: cursor.description — list of 7-tuples
            (name, type_code, display_size, internal_size, precision, scale, null_ok).

    Returns:
        Dict mapping column name to Polars DataType.
    """
    schema: dict[str, pl.DataType] = {}
    for col_desc in description:
        col_name = col_desc[0]
        type_code = col_desc[1]  # Python type object (str, int, datetime.datetime, …)
        schema[col_name] = _PYODBC_TYPE_MAP.get(type_code, pl.String)
    return schema


def _rows_to_dataframe(
    columns: list[str],
    rows: list,
    cursor_schema: dict[str, pl.DataType] | None = None,
) -> pl.DataFrame:
    """Convert fetched rows to a Polars DataFrame using columnar construction.

    This is significantly faster than the dict-per-row approach because it avoids
    creating N Python dicts and lets Polars ingest columnar data directly.

    When cursor_schema is provided (from pyodbc cursor.description), any column
    that Polars infers as pl.Null (all values null) is cast to the declared SQL
    Server type.  This prevents VOID columns in downstream Parquet/Delta.
    """
    if not rows:
        # Use cursor_schema types when available, fall back to Utf8
        schema = {
            col: cursor_schema.get(col, pl.Utf8) if cursor_schema else pl.Utf8
            for col in columns
        }
        return pl.DataFrame(schema=schema)

    col_data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
    df = pl.DataFrame(col_data, infer_schema_length=None)

    # Fix Null-typed columns using the declared SQL Server types
    if cursor_schema:
        casts = [
            pl.col(col).cast(cursor_schema[col], strict=False)
            for col in df.columns
            if df[col].dtype == pl.Null and col in cursor_schema
        ]
        if casts:
            df = df.with_columns(casts)

    return df

try:
    from kantar_db_handler.configs import get_country_params

    KANTAR_DB_HANDLER_AVAILABLE = True
except ImportError:
    KANTAR_DB_HANDLER_AVAILABLE = False


class SQLServerClient:
    """SQL Server database client with country-based configuration support."""

    def __init__(
        self,
        settings: SQLServerSettings | None = None,
        country: str | None = None,
        server: str | None = None,
        database: str | None = None,
    ) -> None:
        """Initialize SQL Server client.

        Args:
            settings: SQL Server settings. Uses default settings if not provided.
            country: Country code to load server/database from kantar_db_handler.
            server: Override server hostname.
            database: Override database name.

        Note:
            Priority order: (server + database) > country > settings
            If country is provided and kantar_db_handler is available, it will
            load server/database from country config files.
        """
        self.settings = settings or get_settings().sql_server
        self._engine: Engine | None = None

        # Handle country-based configuration
        if country and KANTAR_DB_HANDLER_AVAILABLE:
            params = get_country_params(country)
            self._server = server or params["server"]
            self._database = database or params["database"]
        else:
            self._server = server or self.settings.host
            self._database = database or self.settings.database

    @property
    def engine(self) -> Engine:
        """Get or create SQLAlchemy engine with dynamic server/database."""
        if self._engine is None:
            connection_url = self._build_connection_url()
            self._engine = create_engine(
                connection_url,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
                pool_recycle=1800,
            )
            # Use READ UNCOMMITTED (NOLOCK) for all read connections to avoid
            # blocking on concurrent writes — safe for analytics extraction.
            # Also set LOCK_TIMEOUT so queries that hit an incompatible lock
            # (e.g. schema lock from concurrent DDL, which NOLOCK doesn't
            # bypass) fail fast with error 1222 instead of hanging forever.
            @event.listens_for(self._engine, "connect")
            def _set_session_options(dbapi_conn, connection_record):
                cursor = dbapi_conn.cursor()
                cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
                lock_secs = int(self.settings.lock_timeout_seconds)
                lock_ms = -1 if lock_secs < 0 else lock_secs * 1000
                cursor.execute(f"SET LOCK_TIMEOUT {lock_ms}")
                cursor.close()

            # pyodbc connections don't expose a socket fd, so we cannot wire
            # SO_KEEPALIVE here. Instead, set a per-statement timeout: any
            # individual query (including each fetchmany chunk) that goes
            # silent past the threshold raises OperationalError, preventing
            # indefinite hangs on dead TCP connections.
            @event.listens_for(self._engine, "connect")
            def _set_query_timeout(dbapi_conn, connection_record):
                try:
                    dbapi_conn.timeout = int(self.settings.query_timeout_seconds)
                except Exception as exc:
                    logger.debug("Could not set pyodbc statement timeout: %s", exc)
        return self._engine

    def _build_connection_url(self) -> str:
        """Build SQLAlchemy connection URL with dynamic server/database.

        Returns:
            SQLAlchemy connection URL string.
        """
        trust_cert = "yes" if self.settings.trust_server_certificate else "no"
        driver_encoded = self.settings.driver.replace(" ", "+")
        keepalive = self.settings.keepalive_seconds

        # ODBC Driver 17/18 for SQL Server respects the 'Keepalive' connection
        # string keyword (TCP keep-alive interval in seconds). Without it the
        # driver inherits the Windows registry default (~2 hours), which lets
        # fetchmany() hang indefinitely on a dead VPN/NAT/switch session.
        keepalive_param = f"&Keepalive={keepalive}" if keepalive > 0 else ""

        if self.settings.use_windows_auth:
            return (
                f"mssql+pyodbc://@{self._server}:{self.settings.port}/{self._database}"
                f"?driver={driver_encoded}&TrustServerCertificate={trust_cert}"
                f"&Trusted_Connection=yes{keepalive_param}"
            )
        return (
            f"mssql+pyodbc://{self.settings.username}:{self.settings.password.get_secret_value()}"
            f"@{self._server}:{self.settings.port}/{self._database}"
            f"?driver={driver_encoded}&TrustServerCertificate={trust_cert}{keepalive_param}"
        )

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

            # Extract declared column types from pyodbc cursor
            cursor_schema = None
            cursor = getattr(result, "cursor", None)
            if cursor and getattr(cursor, "description", None):
                cursor_schema = _schema_from_cursor(cursor.description)

            rows = result.fetchall()

        return _rows_to_dataframe(columns, rows, cursor_schema)

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

            # Extract declared column types from pyodbc cursor
            cursor_schema = None
            cursor = getattr(result, "cursor", None)
            if cursor and getattr(cursor, "description", None):
                cursor_schema = _schema_from_cursor(cursor.description)

            # Infer schema from first chunk with larger sample
            first_chunk_rows = result.fetchmany(chunk_size)
            if not first_chunk_rows:
                return

            # Create first DataFrame and capture its schema
            first_df = _rows_to_dataframe(columns, first_chunk_rows, cursor_schema)
            schema = first_df.schema
            yield first_df

            # Apply same schema to subsequent chunks
            while True:
                rows = result.fetchmany(chunk_size)
                if not rows:
                    break

                chunk_df = _rows_to_dataframe(columns, rows, cursor_schema)

                # Reconcile column types with the reference schema.
                # If the reference has Null (first chunk was all-null) but this
                # chunk has a real type, upgrade the reference schema instead of
                # discarding data by casting to Null.
                for col, dtype in list(schema.items()):
                    if col not in chunk_df.columns:
                        continue
                    chunk_type = chunk_df[col].dtype
                    if chunk_type == dtype:
                        continue
                    if dtype == pl.Null:
                        # Upgrade: first chunk was all-null, now we know the real type
                        schema[col] = chunk_type
                    else:
                        try:
                            chunk_df = chunk_df.with_columns(
                                pl.col(col).cast(dtype, strict=False)
                            )
                        except Exception:
                            pass

                yield chunk_df

    def execute_query_to_disk(
        self,
        query: str,
        output_dir: Path,
        params: dict[str, Any] | None = None,
        chunk_size: int = 100_000,
        on_progress: Callable[[int, int], None] | None = None,
        is_cancelled: Callable[[], bool] | None = None,
    ) -> tuple[list[Path], int]:
        """Execute query and save each chunk as a parquet file on disk.

        Args:
            query: SQL query to execute.
            output_dir: Directory to write part_NNNNN.parquet files.
            params: Query parameters.
            chunk_size: Number of rows per chunk/parquet file.
            on_progress: Optional callback(chunk_rows, total_rows_so_far).
            is_cancelled: Optional callable that returns True if the job was cancelled.

        Returns:
            Tuple of (list of parquet file paths, total row count).
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        parts: list[Path] = []
        total_rows = 0
        schema = None
        null_upgraded: dict[str, pl.DataType] = {}  # col -> real type discovered later
        t_download_start = time.monotonic()
        stall_threshold = 120  # warn if a single fetchmany takes longer

        with self.engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            columns = list(result.keys())
            part_num = 0

            # Extract declared column types from pyodbc cursor
            cursor_schema = None
            cursor = getattr(result, "cursor", None)
            if cursor and getattr(cursor, "description", None):
                cursor_schema = _schema_from_cursor(cursor.description)

            while True:
                if is_cancelled and is_cancelled():
                    logger.info("Download cancelled, stopping fetch")
                    break

                t_fetch = time.monotonic()
                rows = result.fetchmany(chunk_size)
                fetch_secs = time.monotonic() - t_fetch

                if not rows:
                    break

                if fetch_secs > stall_threshold:
                    logger.warning(
                        "Slow fetch: chunk %d took %.1fs (%d rows) — possible network stall",
                        part_num, fetch_secs, len(rows),
                    )

                chunk_df = _rows_to_dataframe(columns, rows, cursor_schema)

                # Reconcile schema with first chunk
                if schema is None:
                    schema = chunk_df.schema
                else:
                    for col, dtype in list(schema.items()):
                        if col not in chunk_df.columns:
                            continue
                        chunk_type = chunk_df[col].dtype
                        if chunk_type == dtype:
                            continue
                        if dtype == pl.Null:
                            schema[col] = chunk_type
                            null_upgraded[col] = chunk_type
                        else:
                            try:
                                chunk_df = chunk_df.with_columns(
                                    pl.col(col).cast(dtype, strict=False)
                                )
                            except Exception:
                                pass

                part_path = output_dir / f"part_{part_num:05d}.parquet"
                chunk_df.write_parquet(part_path)
                parts.append(part_path)
                total_rows += len(chunk_df)
                part_num += 1

                elapsed = time.monotonic() - t_download_start
                rate = total_rows / elapsed if elapsed > 0 else 0
                logger.info(
                    "Download chunk %d: %s rows (total: %s, %.0f rows/sec, %.1fs elapsed)",
                    part_num, f"{len(chunk_df):,}", f"{total_rows:,}", rate, elapsed,
                )

                if on_progress:
                    on_progress(len(chunk_df), total_rows)

        # Rewrite earlier parts whose Null-typed columns were later discovered
        # to have a real type, so all parquet files share a consistent schema.
        if null_upgraded:
            for part_path in parts:
                part_df = pl.read_parquet(part_path)
                casts = [
                    pl.col(col).cast(real_type, strict=False)
                    for col, real_type in null_upgraded.items()
                    if col in part_df.columns and part_df[col].dtype == pl.Null
                ]
                if casts:
                    part_df.with_columns(casts).write_parquet(part_path)

        return parts, total_rows

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
