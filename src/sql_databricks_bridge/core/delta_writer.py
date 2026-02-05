"""Delta table writer using stage-then-CTAS pattern."""

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

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
    local_path: str | None = None  # Path to local parquet file if saved


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

        If schema is not provided, uses country as schema name.

        Returns:
            "`{catalog}`.`{schema}`.`{query_name}`"
        """
        cat = catalog or self._settings.catalog
        sch = schema or country  # Use country as schema if not provided
        return f"`{cat}`.`{sch}`.`{query_name}`"

    def write_dataframe(
        self,
        df: pl.DataFrame,
        query_name: str,
        country: str,
        catalog: str | None = None,
        schema: str | None = None,
        save_local: bool = False,
    ) -> WriteResult:
        """Write DataFrame to a Delta table using stage-then-CTAS.

        Args:
            df: DataFrame to write.
            query_name: Query name (becomes part of table name).
            country: Country code prefix.
            catalog: Override default catalog.
            schema: Override default schema.
            save_local: If True, save DataFrame as local Parquet file.

        Returns:
            WriteResult with table name, row count, duration, and optional local path.
        """
        start_time = datetime.utcnow()

        table_name = self.resolve_table_name(query_name, country, catalog, schema)

        # Build staging path using target catalog/schema (not defaults from env)
        target_catalog = catalog or self._settings.catalog
        target_schema = schema or country  # Use country as schema if not provided
        staging_volume_path = f"/Volumes/{target_catalog}/{target_schema}/{self._settings.volume}"
        staging_path = f"{staging_volume_path}/_staging/{query_name}.parquet"

        # Ensure target schema exists (Unity Catalog)
        self._ensure_schema(target_catalog, target_schema)

        rows = len(df)
        local_path = None

        # Save locally if requested
        if save_local:
            local_path = self._save_local_parquet(
                df, query_name, country, target_catalog, target_schema
            )

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
            return WriteResult(
                table_name=table_name, rows=0, duration_seconds=duration, local_path=local_path
            )

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
        return WriteResult(
            table_name=table_name, rows=rows, duration_seconds=duration, local_path=local_path
        )

    def _ensure_schema(self, catalog: str, schema: str) -> None:
        """Create schema if it does not exist.

        This is safe/idempotent, and prevents extraction failures when the
        configured destination schema hasn't been created yet.
        """
        try:
            self.client.execute_sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
        except Exception as e:
            # Best-effort: schema might exist but caller lacks permissions; surface the error
            logger.error(f"Failed ensuring schema {catalog}.{schema}: {e}")
            raise

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

    def _save_local_parquet(
        self, df: pl.DataFrame, query_name: str, country: str, catalog: str, schema: str
    ) -> str:
        """Save DataFrame as local Parquet file.

        Args:
            df: DataFrame to save.
            query_name: Query name.
            country: Country code.
            catalog: Catalog name.
            schema: Schema name.

        Returns:
            Path to saved Parquet file.
        """
        # Build path: ~/Documents/projects/data/{catalog}/{schema}/{country}/{query_name}.parquet
        home = Path.home()
        local_dir = home / "Documents" / "projects" / "data" / catalog / schema / country
        local_dir.mkdir(parents=True, exist_ok=True)

        local_file = local_dir / f"{query_name}.parquet"
        df.write_parquet(local_file)

        logger.info(f"Saved {len(df)} rows to local file: {local_file}")
        return str(local_file)
