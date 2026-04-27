"""Delta table writer using stage-then-CTAS pattern."""

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from sql_databricks_bridge.core.composite_columns import databricks_where_in, parse_columns
from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.core.schema_cast import cast_expr, get_parquet_schema
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

    @staticmethod
    def _fix_null_columns(df: pl.DataFrame) -> pl.DataFrame:
        """Cast any pl.Null columns to pl.String to prevent VOID in Parquet/Delta.

        This is a safety net for DataFrames that bypass sql_server.py's cursor
        schema enforcement (e.g., from external sources or simulador sync).
        """
        null_cols = [col for col in df.columns if df[col].dtype == pl.Null]
        if null_cols:
            logger.warning(
                "Casting %d Null-typed columns to String to prevent VOID: %s",
                len(null_cols),
                null_cols,
            )
            df = df.with_columns([pl.col(c).cast(pl.String) for c in null_cols])
        return df

    def resolve_table_name(
        self,
        query_name: str,
        country: str,
        catalog: str | None = None,
        schema: str | None = None,
        table_suffix: str | None = None,
    ) -> str:
        """Build fully-qualified Delta table name.

        If schema is not provided, uses country as schema name.
        If table_suffix is provided, it is appended to the query name
        (e.g. query_name="j_atoscompra_new", table_suffix="_full"
         -> "j_atoscompra_new_full").

        Returns:
            "`{catalog}`.`{schema}`.`{name}`"
        """
        cat = catalog or self._settings.catalog
        sch = schema or country  # Use country as schema if not provided
        name = f"{query_name}{table_suffix}" if table_suffix else query_name
        return f"`{cat}`.`{sch}`.`{name}`"

    def write_dataframe(
        self,
        df: pl.DataFrame,
        query_name: str,
        country: str,
        catalog: str | None = None,
        schema: str | None = None,
        save_local: bool = False,
        tag: str | None = None,
        table_suffix: str | None = None,
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
        df = self._fix_null_columns(df)

        table_name = self.resolve_table_name(query_name, country, catalog, schema, table_suffix)

        # Build staging path using target catalog/schema (not defaults from env)
        # NOTE: read_files() requires a DIRECTORY path, not a single file.
        target_catalog = catalog or self._settings.catalog
        target_schema = schema or country  # Use country as schema if not provided
        staging_volume_path = f"/Volumes/{target_catalog}/{target_schema}/{self._settings.volume}"
        effective_name = f"{query_name}{table_suffix}" if table_suffix else query_name
        staging_dir = f"{staging_volume_path}/_staging/{effective_name}"
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

        # Standard TBLPROPERTIES for all tables created by the bridge
        _tblprops = (
            "TBLPROPERTIES ("
            "'delta.checkpoint.writeStatsAsJson' = 'false', "
            "'delta.checkpoint.writeStatsAsStruct' = 'true', "
            "'delta.parquet.compression.codec' = 'zstd', "
            "'delta.enableDeletionVectors' = 'true', "
            "'delta.logRetentionDuration' = 'interval 1825 days', "
            "'delta.deletedFileRetentionDuration' = 'interval 1825 days'"
            ")"
        )

        if rows == 0:
            if self.table_exists(table_name):
                # Table exists — truncate to preserve time travel history
                self.client.execute_sql(f"TRUNCATE TABLE {table_name}")
                logger.info(f"Truncated existing table {table_name} (0 rows)")
            else:
                # Create empty table with correct schema using LIMIT 0
                self.client.upload_dataframe(df, staging_file)
                ctas = (
                    f"CREATE TABLE {table_name} {_tblprops} "
                    f"AS SELECT * EXCEPT(_rescued_data) FROM read_files('{staging_dir}', format => 'parquet') LIMIT 0"
                )
                self.client.execute_sql(ctas)
                logger.info(f"Created empty table {table_name}")
            self._apply_tags(table_name, tag)
            self._cleanup_staging(staging_file)
            duration = (datetime.utcnow() - start_time).total_seconds()
            return WriteResult(
                table_name=table_name, rows=0, duration_seconds=duration, local_path=local_path
            )

        # 1. Stage parquet (single file inside a directory for read_files)
        self.client.upload_dataframe(df, staging_file)
        logger.info(f"Staged {rows} rows to {staging_dir}")

        # 2. Write data to Delta table
        # Use INSERT OVERWRITE for existing tables to preserve time travel history.
        # Only use CREATE TABLE for genuinely new tables.
        if self.table_exists(table_name):
            # Schema-aware overwrite: source parquet may have fewer/different
            # columns than the target Delta (e.g. simulador VOLEQ files where
            # the unit set varies by period). Build the SELECT list from the
            # target schema, emitting CAST(NULL AS T) for missing source cols
            # and TRY_CAST for cross-family type drift.
            target_schema = self._get_table_schema(table_name)
            if target_schema:
                source_schema = get_parquet_schema(self.client, staging_dir)
                cols = [c for c in target_schema if c.lower() != "_rescued_data"]
                col_list = ", ".join(f"`{c}`" for c in cols)
                cast_list = ", ".join(
                    cast_expr(c, source_schema.get(c), target_schema[c]) for c in cols
                )
                insert_sql = (
                    f"INSERT OVERWRITE {table_name} ({col_list}) "
                    f"SELECT {cast_list} FROM read_files('{staging_dir}', format => 'parquet')"
                )
            else:
                insert_sql = (
                    f"INSERT OVERWRITE {table_name} "
                    f"SELECT * EXCEPT(_rescued_data) FROM read_files('{staging_dir}', format => 'parquet')"
                )
            self.client.execute_sql(insert_sql)
            logger.info(f"Overwrote table {table_name} with {rows} rows (INSERT OVERWRITE)")
        else:
            ctas = (
                f"CREATE TABLE {table_name} {_tblprops} "
                f"AS SELECT * EXCEPT(_rescued_data) FROM read_files('{staging_dir}', format => 'parquet')"
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

    def append_dataframe(
        self,
        df: pl.DataFrame,
        query_name: str,
        country: str,
        catalog: str | None = None,
        schema: str | None = None,
        tag: str | None = None,
        table_suffix: str | None = None,
    ) -> WriteResult:
        """Append DataFrame rows to an existing Delta table (INSERT INTO).

        If the table doesn't exist yet, falls back to write_dataframe (CTAS).

        Args:
            df: DataFrame to append.
            query_name: Query name (table name in Delta).
            country: Country code.
            catalog: Override catalog.
            schema: Override schema.
            tag: Tag for the table version.

        Returns:
            WriteResult with table name, row count, duration.
        """
        start_time = datetime.utcnow()

        # Lowercase column names for consistency
        df = df.rename({col: col.lower() for col in df.columns})
        df = self._fix_null_columns(df)

        table_name = self.resolve_table_name(query_name, country, catalog, schema, table_suffix)
        target_catalog = catalog or self._settings.catalog
        target_schema = schema or country
        staging_volume_path = f"/Volumes/{target_catalog}/{target_schema}/{self._settings.volume}"
        effective_name = f"{query_name}{table_suffix}" if table_suffix else query_name
        staging_dir = f"{staging_volume_path}/_staging_incr/{effective_name}"
        staging_file = f"{staging_dir}/data.parquet"

        self._ensure_schema(target_catalog, target_schema)

        rows = len(df)
        if rows == 0:
            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"No new rows to append to {table_name}")
            return WriteResult(table_name=table_name, rows=0, duration_seconds=duration)

        # If table doesn't exist, do full write (CTAS)
        if not self.table_exists(table_name):
            logger.info(f"Table {table_name} doesn't exist, creating with {rows} rows")
            return self.write_dataframe(
                df, query_name, country, catalog, schema, tag=tag, table_suffix=table_suffix,
            )

        # Stage parquet
        self._clean_staging_dir(staging_dir)
        self.client.upload_dataframe(df, staging_file)
        logger.info(f"Staged {rows} rows for append to {staging_dir}")

        # INSERT INTO with explicit column list (exclude _rescued_data)
        target_cols = self._get_table_columns(table_name)
        if target_cols:
            cols = [c for c in target_cols if c.lower() != "_rescued_data"]
            col_list = ", ".join(f"`{c}`" for c in cols)
            insert_sql = (
                f"INSERT INTO {table_name} ({col_list}) "
                f"SELECT {col_list} FROM read_files('{staging_dir}', format => 'parquet')"
            )
        else:
            insert_sql = (
                f"INSERT INTO {table_name} "
                f"SELECT * EXCEPT(_rescued_data) FROM read_files('{staging_dir}', format => 'parquet')"
            )

        try:
            self.client.execute_sql(insert_sql)
            logger.info(f"Appended {rows} rows to {table_name}")
        except RuntimeError as e:
            err_msg = str(e)
            if any(code in err_msg for code in [
                "DATATYPE_MISMATCH", "CAST_WITHOUT_SUGGESTION",
                "DELTA_DUPLICATE_COLUMNS_FOUND",
                "UNRESOLVED_COLUMN",
            ]):
                logger.warning(f"Schema issue on append to {table_name}, falling back to OVERWRITE: {e}")
                return self.write_dataframe(
                    df, query_name, country, catalog, schema, tag=tag, table_suffix=table_suffix,
                )
            raise

        # Tags + cleanup
        self._apply_tags(table_name, tag)
        self._cleanup_staging(staging_file)

        duration = (datetime.utcnow() - start_time).total_seconds()
        return WriteResult(table_name=table_name, rows=rows, duration_seconds=duration)

    def get_max_key(self, table_name: str, key_column: str) -> int | str | None:
        """Get MAX(key_column) from a Delta table for incremental sync watermark.

        Returns:
            The max key value, or None if table doesn't exist or is empty.
            Numeric columns are returned as int; datetime/timestamp columns are
            returned as ISO string so callers can quote them in SQL predicates.
        """
        try:
            if not self.table_exists(table_name):
                return None
            rows = self.client.execute_sql(
                f"SELECT MAX(`{key_column}`) AS max_key FROM {table_name}"
            )
            if not rows or rows[0].get("max_key") is None:
                return None
            raw = rows[0]["max_key"]
            # Numeric → return as int (preserve existing behaviour)
            if isinstance(raw, (int, float)):
                return int(raw)
            s = str(raw).strip()
            # Try to parse numeric-looking strings as int for backwards compat
            try:
                return int(s)
            except ValueError:
                # Datetime / other: return raw string so caller can quote it
                return s
        except Exception as e:
            logger.warning(f"Failed to get MAX({key_column}) from {table_name}: {e}")
            return None

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
        table_suffix: str | None = None,
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
        table_name = self.resolve_table_name(query_name, country, catalog, schema, table_suffix)
        target_catalog = catalog or self._settings.catalog
        target_schema = schema or country
        staging_volume_path = f"/Volumes/{target_catalog}/{target_schema}/{self._settings.volume}"
        effective_name = f"{query_name}{table_suffix}" if table_suffix else query_name
        staging_dir = f"{staging_volume_path}/_staging_diff/{effective_name}"

        self._ensure_schema(target_catalog, target_schema)

        total_rows = sum(len(c) for c in chunks)

        # If table doesn't exist yet, fall back to full write
        if not self.table_exists(table_name):
            logger.info(f"Table {table_name} doesn't exist, doing full write")
            if chunks:
                combined = pl.concat(chunks) if len(chunks) > 1 else chunks[0]
                return self.write_dataframe(
                    combined, query_name, country, catalog, schema,
                    tag=tag, table_suffix=table_suffix,
                )
            return WriteResult(table_name=table_name, rows=0, duration_seconds=0)

        if not chunks and not deleted_level1_values:
            logger.info(f"No changes to write for {table_name}")
            duration = (datetime.utcnow() - start_time).total_seconds()
            return WriteResult(table_name=table_name, rows=0, duration_seconds=duration)

        # 1. Upload chunks to staging (clean directory first to avoid stale files)
        if chunks:
            # Lowercase column names for consistency + fix Null columns
            chunks = [
                self._fix_null_columns(c.rename({col: col.lower() for col in c.columns}))
                for c in chunks
            ]
            # Clean staging directory to prevent stale files with different schemas
            self._clean_staging_dir(staging_dir)
            uploaded_paths = self.client.upload_dataframe_chunked(chunks, staging_dir)
            logger.info(f"Staged {total_rows} rows in {len(uploaded_paths)} parts to {staging_dir}")

        # 2. DELETE changed slices from target
        try:
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

            # Delete entire level1 values (batched into single IN clause)
            if deleted_level1_values:
                l1_cols = parse_columns(level1_column)
                batch_size = 500
                for i in range(0, len(deleted_level1_values), batch_size):
                    batch = deleted_level1_values[i:i + batch_size]
                    where_clause = databricks_where_in(l1_cols, batch)
                    delete_sql = f"DELETE FROM {table_name} WHERE {where_clause}"
                    self.client.execute_sql(delete_sql)
                logger.info(f"Deleted {len(deleted_level1_values)} {level1_column} values from {table_name}")
        except RuntimeError as e:
            if "UNRESOLVED_COLUMN" in str(e):
                logger.warning(
                    f"DELETE failed on {table_name} (column not found), "
                    f"falling back to full OVERWRITE: {e}"
                )
                if chunks:
                    combined = pl.concat(chunks) if len(chunks) > 1 else chunks[0]
                    return self.write_dataframe(
                        combined, query_name, country, catalog, schema,
                        tag=tag, table_suffix=table_suffix,
                    )
            raise

        # 3. INSERT from staging (use column intersection to handle schema changes)
        if chunks:
            target_schema_map = self._get_table_schema(table_name)
            # Get parquet column names from the chunks
            parquet_cols_set = set()
            for c in chunks:
                parquet_cols_set.update(col.lower() for col in c.columns)

            if target_schema_map and parquet_cols_set:
                target_cols_set = {
                    c.lower() for c in target_schema_map if c.lower() != "_rescued_data"
                }
                # Use intersection: columns in BOTH target table AND parquet
                common_cols = [
                    c for c in target_schema_map
                    if c.lower() in parquet_cols_set and c.lower() != "_rescued_data"
                ]
                extra_in_target = target_cols_set - parquet_cols_set
                extra_in_parquet = parquet_cols_set - target_cols_set
                if extra_in_target or extra_in_parquet:
                    logger.warning(
                        f"Schema mismatch for {table_name}: "
                        f"extra in Delta: {extra_in_target or 'none'}, "
                        f"extra in data: {extra_in_parquet or 'none'}. "
                        f"Falling back to full OVERWRITE."
                    )
                    combined = pl.concat(chunks) if len(chunks) > 1 else chunks[0]
                    return self.write_dataframe(
                        combined, query_name, country, catalog, schema,
                        tag=tag, table_suffix=table_suffix,
                    )
                col_list = ", ".join(f"`{c}`" for c in common_cols)
                cast_list = ", ".join(
                    f"CAST(`{c}` AS {target_schema_map[c]}) AS `{c}`"
                    for c in common_cols
                )
                insert_sql = (
                    f"INSERT INTO {table_name} ({col_list}) "
                    f"SELECT {cast_list} FROM read_files('{staging_dir}', format => 'parquet')"
                )
            else:
                # Fallback: SELECT * EXCEPT to avoid _rescued_data duplication
                insert_sql = (
                    f"INSERT INTO {table_name} "
                    f"SELECT * EXCEPT(_rescued_data) FROM read_files('{staging_dir}', format => 'parquet')"
                )
            try:
                self.client.execute_sql(insert_sql)
                logger.info(f"Inserted {total_rows} rows into {table_name}")
            except RuntimeError as e:
                err_msg = str(e)
                if any(code in err_msg for code in [
                    "DATATYPE_MISMATCH", "CAST_WITHOUT_SUGGESTION",
                    "DELTA_DUPLICATE_COLUMNS_FOUND",
                    "UNRESOLVED_COLUMN",
                ]):
                    logger.warning(
                        f"Schema issue on INSERT INTO {table_name}, "
                        f"falling back to full OVERWRITE: {e}"
                    )
                    # Re-create table with correct schema from current data
                    combined = pl.concat(chunks) if len(chunks) > 1 else chunks[0]
                    return self.write_dataframe(
                        combined, query_name, country, catalog, schema,
                        tag=tag, table_suffix=table_suffix,
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

    def _clean_staging_dir(self, staging_dir: str) -> None:
        """Remove all existing files from staging directory before upload."""
        try:
            files = self.client.list_files(staging_dir)
            for f in files:
                try:
                    self.client.delete_file(f.path)
                except Exception:
                    pass
        except Exception:
            pass  # Directory may not exist yet

    def _get_table_columns(self, table_name: str) -> list[str]:
        """Get column names from an existing Delta table."""
        try:
            rows = self.client.execute_sql(f"DESCRIBE TABLE {table_name}")
            return [row["col_name"] for row in rows if row.get("col_name") and not row["col_name"].startswith("#")]
        except Exception:
            return []

    def _get_table_schema(self, table_name: str) -> dict[str, str]:
        """Get column name -> data_type mapping from an existing Delta table."""
        try:
            rows = self.client.execute_sql(f"DESCRIBE TABLE {table_name}")
            return {
                row["col_name"]: row["data_type"]
                for row in rows
                if row.get("col_name") and not row["col_name"].startswith("#")
            }
        except Exception:
            return {}

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
