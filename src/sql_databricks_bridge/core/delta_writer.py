"""Delta table writer using stage-then-CTAS pattern."""

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

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
        tag: str | None = None,
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

        # Convert all column names to lowercase for Databricks consistency
        df = df.rename({col: col.lower() for col in df.columns})

        table_name = self.resolve_table_name(query_name, country, catalog, schema)

        # Build staging path using target catalog/schema (not defaults from env)
        # NOTE: read_files() requires a DIRECTORY path, not a single file.
        target_catalog = catalog or self._settings.catalog
        target_schema = schema or country  # Use country as schema if not provided
        staging_volume_path = f"/Volumes/{target_catalog}/{target_schema}/{self._settings.volume}"
        staging_dir = f"{staging_volume_path}/_staging/{query_name}"
        staging_file = f"{staging_dir}/data.parquet"

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
            self.client.upload_dataframe(df, staging_file)
            ctas = (
                f"CREATE OR REPLACE TABLE {table_name} "
                f"AS SELECT * FROM read_files('{staging_dir}', format => 'parquet') LIMIT 0"
            )
            self.client.execute_sql(ctas)
            self._apply_tags(table_name, tag)
            self._cleanup_staging(staging_file)
            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"Created empty table {table_name}")
            return WriteResult(
                table_name=table_name, rows=0, duration_seconds=duration, local_path=local_path
            )

        # 1. Stage parquet (single file inside a directory for read_files)
        self.client.upload_dataframe(df, staging_file)
        logger.info(f"Staged {rows} rows to {staging_dir}")

        # 2. CTAS (read_files reads all parquet files in the directory)
        ctas = (
            f"CREATE OR REPLACE TABLE {table_name} "
            f"AS SELECT * FROM read_files('{staging_dir}', format => 'parquet')"
        )
        self.client.execute_sql(ctas)
        logger.info(f"Created table {table_name} with {rows} rows")

        # 3. Apply tags (best-effort)
        self._apply_tags(table_name, tag)

        # 4. Cleanup staging (best-effort)
        self._cleanup_staging(staging_file)

        duration = (datetime.utcnow() - start_time).total_seconds()
        return WriteResult(
            table_name=table_name, rows=rows, duration_seconds=duration, local_path=local_path
        )

    def write_diff_slices(
        self,
        chunks: list[pl.DataFrame],
        query_name: str,
        country: str,
        changed_pairs: list[tuple[str, str]],
        level1_column: str,
        level2_column: str,
        deleted_level1_values: list[str] | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        tag: str | None = None,
    ) -> WriteResult:
        """Write only changed slices using DELETE + INSERT pattern.

        1. Upload chunks as parquet parts to staging Volume
        2. DELETE rows for changed (level1, level2) pairs + deleted level1 values
        3. INSERT from staged parquet files
        4. Cleanup staging

        Args:
            chunks: DataFrames to insert (only changed rows).
            query_name: Query name (table name in Delta).
            country: Country code.
            changed_pairs: List of (level1_value, level2_value) to replace.
            level1_column: Column name for level 1 (e.g., 'periodo').
            level2_column: Column name for level 2 (e.g., 'idproduto').
            deleted_level1_values: Level 1 values entirely removed from source.
            catalog: Override catalog.
            schema: Override schema.
            tag: Tag for the table version.

        Returns:
            WriteResult with table name, row count, duration.
        """
        start_time = datetime.utcnow()
        table_name = self.resolve_table_name(query_name, country, catalog, schema)
        target_catalog = catalog or self._settings.catalog
        target_schema = schema or country
        staging_volume_path = f"/Volumes/{target_catalog}/{target_schema}/{self._settings.volume}"
        staging_dir = f"{staging_volume_path}/_staging_diff/{query_name}"

        self._ensure_schema(target_catalog, target_schema)

        total_rows = sum(len(c) for c in chunks)

        # If table doesn't exist yet, fall back to full write
        if not self.table_exists(table_name):
            logger.info(f"Table {table_name} doesn't exist, doing full write")
            if chunks:
                combined = pl.concat(chunks) if len(chunks) > 1 else chunks[0]
                return self.write_dataframe(combined, query_name, country, catalog, schema, tag=tag)
            return WriteResult(table_name=table_name, rows=0, duration_seconds=0)

        if not chunks and not deleted_level1_values:
            logger.info(f"No changes to write for {table_name}")
            duration = (datetime.utcnow() - start_time).total_seconds()
            return WriteResult(table_name=table_name, rows=0, duration_seconds=duration)

        # 1. Upload chunks to staging
        if chunks:
            # Lowercase column names for consistency
            chunks = [c.rename({col: col.lower() for col in c.columns}) for c in chunks]
            uploaded_paths = self.client.upload_dataframe_chunked(chunks, staging_dir)
            logger.info(f"Staged {total_rows} rows in {len(uploaded_paths)} parts to {staging_dir}")

        # 2. DELETE changed slices from target
        if changed_pairs:
            # Build WHERE clause for changed pairs
            pair_conditions = []
            for l1, l2 in changed_pairs:
                pair_conditions.append(
                    f"(CAST({level1_column} AS STRING) = '{l1}' AND CAST({level2_column} AS STRING) = '{l2}')"
                )

            # Batch delete conditions to avoid SQL size limits
            batch_size = 200
            for i in range(0, len(pair_conditions), batch_size):
                batch = pair_conditions[i:i + batch_size]
                delete_sql = f"DELETE FROM {table_name} WHERE {' OR '.join(batch)}"
                self.client.execute_sql(delete_sql)

            logger.info(f"Deleted {len(changed_pairs)} changed slices from {table_name}")

        # Delete entire level1 values that were removed from source
        if deleted_level1_values:
            for val in deleted_level1_values:
                delete_sql = f"DELETE FROM {table_name} WHERE CAST({level1_column} AS STRING) = '{val}'"
                self.client.execute_sql(delete_sql)
            logger.info(f"Deleted {len(deleted_level1_values)} removed {level1_column} values from {table_name}")

        # 3. INSERT from staging
        if chunks:
            insert_sql = (
                f"INSERT INTO {table_name} "
                f"SELECT * FROM read_files('{staging_dir}', format => 'parquet')"
            )
            try:
                self.client.execute_sql(insert_sql)
                logger.info(f"Inserted {total_rows} rows into {table_name}")
            except RuntimeError as e:
                if "DATATYPE_MISMATCH" in str(e) or "CAST_WITHOUT_SUGGESTION" in str(e):
                    logger.warning(
                        f"Schema mismatch on INSERT INTO {table_name}, "
                        f"falling back to full OVERWRITE: {e}"
                    )
                    # Re-create table with correct schema from current data
                    combined = pl.concat(chunks) if len(chunks) > 1 else chunks[0]
                    return self.write_dataframe(
                        combined, query_name, country, catalog, schema, tag=tag
                    )
                raise

        # 4. Apply tags
        self._apply_tags(table_name, tag)

        # 5. Cleanup staging (best-effort)
        if chunks:
            for path in uploaded_paths:
                self._cleanup_staging(path)

        duration = (datetime.utcnow() - start_time).total_seconds()
        return WriteResult(table_name=table_name, rows=total_rows, duration_seconds=duration)

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

    def get_current_version(self, table_name: str) -> int:
        """Get the latest version number of a Delta table.

        Args:
            table_name: Fully-qualified table name (catalog.schema.table).

        Returns:
            Latest version number.

        Raises:
            RuntimeError: If table has no history or cannot be queried.
        """
        rows = self.client.execute_sql(f"DESCRIBE HISTORY {table_name} LIMIT 1")
        if not rows:
            raise RuntimeError(f"No history found for {table_name}")
        return int(rows[0]["version"])

    def get_history(self, table_name: str, limit: int = 20) -> list[dict]:
        """Get Delta table version history.

        Args:
            table_name: Fully-qualified table name (catalog.schema.table).
            limit: Maximum number of history entries to return.

        Returns:
            List of history row dicts with version, timestamp, operation, etc.
        """
        return self.client.execute_sql(f"DESCRIBE HISTORY {table_name} LIMIT {limit}")

    def _apply_tags(self, table_name: str, tag: str | None) -> None:
        """Apply Unity Catalog tags to a Delta table (best-effort)."""
        if not tag:
            return
        try:
            self.client.execute_sql(
                f"ALTER TABLE {table_name} SET TAGS ('sync_tag' = '{tag}')"
            )
            logger.info(f"Applied tag '{tag}' to {table_name}")
        except Exception as e:
            logger.warning(f"Failed to apply tags to {table_name}: {e}")

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
