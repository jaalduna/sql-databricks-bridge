"""Delta table writer using stage-then-CTAS pattern."""

import logging
from dataclasses import dataclass
from datetime import datetime

import polars as pl

from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.db.databricks import DatabricksClient

logger = logging.getLogger(__name__)


@dataclass
class WriteResult:
    """Result of writing a DataFrame to a Delta table."""

    table_name: str
    rows: int
    duration_seconds: float


class DeltaTableWriter:
    """Writes DataFrames to Databricks Delta tables via stage-then-CTAS.

    Flow:
        1. Upload DataFrame as temporary parquet to Volume staging path
        2. CREATE OR REPLACE TABLE ... AS SELECT * FROM read_files(staging_path)
        3. Delete staging file (best-effort)
    """

    def __init__(self, client: DatabricksClient | None = None) -> None:
        self.client = client or DatabricksClient()
        self._settings = get_settings().databricks

    def resolve_table_name(
        self,
        query_name: str,
        country: str,
        catalog: str | None = None,
        schema: str | None = None,
    ) -> str:
        """Build fully-qualified Delta table name.

        Returns:
            "`{catalog}`.`{schema}`.`{country}_{query_name}`"
        """
        cat = catalog or self._settings.catalog
        sch = schema or self._settings.schema_name
        return f"`{cat}`.`{sch}`.`{country}_{query_name}`"

    def write_dataframe(
        self,
        df: pl.DataFrame,
        query_name: str,
        country: str,
        catalog: str | None = None,
        schema: str | None = None,
    ) -> WriteResult:
        """Write DataFrame to a Delta table using stage-then-CTAS.

        Args:
            df: DataFrame to write.
            query_name: Query name (becomes part of table name).
            country: Country code prefix.
            catalog: Override default catalog.
            schema: Override default schema.

        Returns:
            WriteResult with table name, row count, and duration.
        """
        start_time = datetime.utcnow()

        table_name = self.resolve_table_name(query_name, country, catalog, schema)

        # Build staging path using target catalog/schema (not defaults from env)
        target_catalog = catalog or self._settings.catalog
        target_schema = schema or self._settings.schema_name
        staging_volume_path = f"/Volumes/{target_catalog}/{target_schema}/{self._settings.volume}"
        staging_path = f"{staging_volume_path}/_staging/{country}_{query_name}.parquet"

        rows = len(df)

        if rows == 0:
            # Create empty table with correct schema using LIMIT 0
            self.client.upload_dataframe(df, staging_path)
            ctas = (
                f"CREATE OR REPLACE TABLE {table_name} "
                f"AS SELECT * FROM read_files('{staging_path}', format => 'parquet') LIMIT 0"
            )
            self.client.execute_sql(ctas)
            self._cleanup_staging(staging_path)
            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"Created empty table {table_name}")
            return WriteResult(table_name=table_name, rows=0, duration_seconds=duration)

        # 1. Stage parquet
        self.client.upload_dataframe(df, staging_path)
        logger.info(f"Staged {rows} rows to {staging_path}")

        # 2. CTAS
        ctas = (
            f"CREATE OR REPLACE TABLE {table_name} "
            f"AS SELECT * FROM read_files('{staging_path}', format => 'parquet')"
        )
        self.client.execute_sql(ctas)
        logger.info(f"Created table {table_name} with {rows} rows")

        # 3. Cleanup staging (best-effort)
        self._cleanup_staging(staging_path)

        duration = (datetime.utcnow() - start_time).total_seconds()
        return WriteResult(table_name=table_name, rows=rows, duration_seconds=duration)

    def table_exists(self, table_name: str) -> bool:
        """Check if a Delta table exists.

        Args:
            table_name: Fully-qualified table name (catalog.schema.table).

        Returns:
            True if table exists.
        """
        try:
            self.client.execute_sql(f"DESCRIBE TABLE {table_name}")
            return True
        except Exception:
            return False

    def _cleanup_staging(self, path: str) -> None:
        """Delete staging file, logging warnings on failure."""
        try:
            self.client.delete_file(path)
        except Exception as e:
            logger.warning(f"Failed to cleanup staging file {path}: {e}")
