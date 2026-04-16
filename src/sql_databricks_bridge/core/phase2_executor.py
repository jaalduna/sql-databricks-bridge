"""Phase 2 executor for two-phase sync.

Drains the :class:`SyncQueue` and executes all queued Databricks SQL
operations in a tight batch.  The SQL warehouse is only ON during this
method — typically 30-60 seconds per country with parallel execution.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sql_databricks_bridge.core.composite_columns import databricks_where_in, parse_columns
from sql_databricks_bridge.core.delta_writer import DeltaTableWriter
from sql_databricks_bridge.core.fingerprint import (
    Fingerprint,
    ensure_fingerprint_table,
    save_fingerprints,
)
from sql_databricks_bridge.core.fingerprint_cache import FingerprintCache
from sql_databricks_bridge.core.sync_queue import SyncQueue
from sql_databricks_bridge.db.databricks import DatabricksClient

logger = logging.getLogger(__name__)


@dataclass
class Phase2Result:
    """Summary of a Phase 2 commit batch."""

    tables_committed: int = 0
    tables_failed: int = 0
    total_rows: int = 0
    fingerprints_saved: int = 0
    duration_seconds: float = 0
    errors: list[dict[str, str]] = field(default_factory=list)


class Phase2Executor:
    """Execute queued Databricks operations in a tight batch."""

    def __init__(
        self,
        dbx_client: DatabricksClient,
        writer: DeltaTableWriter,
        queue: SyncQueue,
        fingerprint_cache: FingerprintCache,
        fingerprint_table: str,
    ) -> None:
        self.dbx_client = dbx_client
        self.writer = writer
        self.queue = queue
        self.fingerprint_cache = fingerprint_cache
        self.fingerprint_table = fingerprint_table

    def execute_batch(
        self,
        job_id: str | None = None,
        on_progress: Any = None,
        max_parallel: int = 4,
    ) -> Phase2Result:
        """Process all pending queue items.  Warehouse ON only during this method.

        Args:
            job_id: If given, only process items for this job.
            on_progress: Optional callback(message: str).
            max_parallel: Max concurrent Databricks SQL operations.

        Returns:
            Phase2Result summary.
        """
        result = Phase2Result()
        start = datetime.utcnow()

        pending = self.queue.get_pending(job_id=job_id)
        if not pending:
            logger.info("Phase 2: nothing to commit")
            return result

        logger.info(
            f"Phase 2: committing {len(pending)} queued operation(s) "
            f"(max_parallel={max_parallel})"
        )

        # Split into table writes (parallelizable) and fingerprint saves
        table_ops = [i for i in pending if i["operation"] in ("ctas", "diff_write")]
        fp_ops = [i for i in pending if i["operation"] == "save_fingerprints"]

        # Ensure fingerprint table once before parallel execution
        if fp_ops:
            ensure_fingerprint_table(self.dbx_client, self.fingerprint_table)

        # --- Phase 2a: parallel table writes (CTAS / diff_write) ---
        if table_ops:
            logger.info(f"Phase 2a: {len(table_ops)} table write(s) with {max_parallel} threads")
            self._execute_parallel(table_ops, result, max_parallel)

        # --- Phase 2b: parallel fingerprint saves ---
        if fp_ops:
            logger.info(f"Phase 2b: {len(fp_ops)} fingerprint save(s) with {max_parallel} threads")
            self._execute_parallel(fp_ops, result, max_parallel)

        result.duration_seconds = (datetime.utcnow() - start).total_seconds()
        logger.info(
            f"Phase 2 complete in {result.duration_seconds:.1f}s: "
            f"{result.tables_committed} committed, {result.tables_failed} failed, "
            f"{result.fingerprints_saved} fingerprint saves"
        )
        return result

    def _execute_parallel(
        self,
        items: list[dict],
        result: Phase2Result,
        max_parallel: int,
    ) -> None:
        """Execute a list of queue items in parallel using a thread pool."""

        def _run_item(item: dict) -> tuple[dict, str | None]:
            """Returns (item, error_msg) — error_msg is None on success."""
            qid = item["id"]
            op = item["operation"]
            country = item["country"]
            table_name = item["table_name"]
            staging_path = item.get("staging_path", "")
            meta = item.get("metadata", {})
            tag = item.get("tag", "")
            table_suffix = item.get("table_suffix", "") or None

            try:
                if op == "ctas":
                    self._execute_ctas(
                        country, table_name, staging_path, tag, table_suffix,
                    )
                elif op == "diff_write":
                    self._execute_diff_write(
                        country, table_name, staging_path, meta, tag, table_suffix,
                    )
                elif op == "save_fingerprints":
                    self._execute_save_fingerprints(
                        country, table_name, meta, item.get("job_id", ""),
                    )

                self.queue.mark_committed(qid)
                logger.info(
                    f"Phase 2: committed queue item {qid} ({op} {country}/{table_name})"
                )
                return (item, None)
            except Exception as e:
                err_msg = str(e)[:500]
                self.queue.mark_failed(qid, err_msg)
                logger.error(
                    f"Phase 2: FAILED queue item {qid} "
                    f"({op} {country}/{table_name}): {e}"
                )
                return (item, err_msg)

        with ThreadPoolExecutor(max_workers=max_parallel) as pool:
            futures = {pool.submit(_run_item, item): item for item in items}
            for future in as_completed(futures):
                item, err_msg = future.result()
                op = item["operation"]
                if err_msg:
                    result.tables_failed += 1
                    result.errors.append({
                        "queue_id": str(item["id"]),
                        "table": item["table_name"],
                        "error": err_msg,
                    })
                elif op in ("ctas", "diff_write"):
                    result.tables_committed += 1
                elif op == "save_fingerprints":
                    result.fingerprints_saved += 1

    # ------------------------------------------------------------------
    # VOID schema fix
    # ------------------------------------------------------------------

    def _fix_void_schema(self, target: str, staging_path: str) -> bool:
        """Detect and fix VOID columns in a Delta table.

        If the target table has VOID-typed columns (from old writes before the
        _schema_from_cursor fix), this method:
          1. Renames the table to ``{target}__old``
          2. Creates a fresh table from the parquet staging data (correct types)
          3. Copies non-VOID data from ``__old`` into the new table
          4. Drops ``__old`` on success; renames it back on failure

        Returns True if a fix was applied, False if no VOID columns found.
        """
        schema = self.writer._get_table_schema(target)
        if not schema:
            return False

        void_cols = [c for c, t in schema.items() if t.upper() == "VOID"]
        if not void_cols:
            return False

        logger.warning(f"VOID columns detected in {target}: {void_cols}")

        # Build __old name: strip trailing backtick, append __old, re-add backtick
        # target looks like `catalog`.`schema`.`name`
        old_target = target[:-1] + "__old`"

        try:
            # 1. Rename existing table to __old
            self.dbx_client.execute_sql(
                f"ALTER TABLE {target} RENAME TO {old_target}"
            )
            logger.info(f"VOID fix: renamed {target} -> {old_target}")

            # 2. Create fresh table with correct schema from parquet
            self.dbx_client.execute_sql(
                f"CREATE TABLE {target} AS "
                f"SELECT * EXCEPT(_rescued_data) "
                f"FROM read_files('{staging_path}', format => 'parquet') "
                f"WHERE 1=0"
            )
            logger.info(f"VOID fix: created empty {target} with correct schema")

            # 3. Copy data from __old, casting VOID columns to STRING
            old_schema = self.writer._get_table_schema(old_target)
            new_schema = self.writer._get_table_schema(target)
            select_parts = []
            for col in old_schema:
                if col.lower() == "_rescued_data":
                    continue
                if col not in new_schema:
                    continue
                if old_schema[col].upper() == "VOID":
                    # Cast VOID -> target type from new schema
                    select_parts.append(
                        f"CAST(NULL AS {new_schema[col]}) AS `{col}`"
                    )
                else:
                    select_parts.append(f"`{col}`")

            if select_parts:
                select_list = ", ".join(select_parts)
                self.dbx_client.execute_sql(
                    f"INSERT INTO {target} SELECT {select_list} FROM {old_target}"
                )
                logger.info(f"VOID fix: copied data from {old_target} -> {target}")

            # 4. Drop __old
            self.dbx_client.execute_sql(f"DROP TABLE {old_target}")
            logger.info(f"VOID fix: dropped {old_target}")
            return True

        except Exception as e:
            logger.error(f"VOID fix failed for {target}: {e}")
            # Attempt recovery: drop the new table and rename __old back
            try:
                self.dbx_client.execute_sql(f"DROP TABLE IF EXISTS {target}")
                self.dbx_client.execute_sql(
                    f"ALTER TABLE {old_target} RENAME TO {target}"
                )
                logger.info(f"VOID fix: recovered — renamed {old_target} back to {target}")
            except Exception as recover_err:
                logger.error(f"VOID fix recovery also failed: {recover_err}")
            raise

    # ------------------------------------------------------------------
    # Operation handlers
    # ------------------------------------------------------------------

    def _execute_ctas(
        self,
        country: str,
        table_name: str,
        staging_path: str,
        tag: str,
        table_suffix: str | None,
    ) -> None:
        """Full overwrite via read_files from pre-staged parquet."""
        target = self.writer.resolve_table_name(table_name, country, table_suffix=table_suffix)
        logger.info(f"Phase 2 CTAS: {target} from {staging_path}")

        # Ensure schema exists
        settings = self.writer._settings
        catalog = settings.catalog
        schema = country
        self.writer._ensure_schema(catalog, schema)

        # Use INSERT OVERWRITE for existing tables to preserve time travel history.
        # Only use CREATE TABLE for genuinely new tables.
        if self.writer.table_exists(target):
            # Fix VOID columns if present (from old writes before schema fix)
            self._fix_void_schema(target, staging_path)

            insert_sql = (
                f"INSERT OVERWRITE {target} "
                f"SELECT * EXCEPT(_rescued_data) "
                f"FROM read_files('{staging_path}', format => 'parquet')"
            )
            self.dbx_client.execute_sql(insert_sql)
            logger.info(f"Phase 2: INSERT OVERWRITE {target} (preserving history)")
        else:
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
            ctas_sql = (
                f"CREATE TABLE {target} {_tblprops} "
                f"AS SELECT * EXCEPT(_rescued_data) "
                f"FROM read_files('{staging_path}', format => 'parquet')"
            )
            self.dbx_client.execute_sql(ctas_sql)
            logger.info(f"Phase 2: created new table {target}")
        self.writer._apply_tags(target, tag)
        self._cleanup_staging(staging_path)

    def _execute_diff_write(
        self,
        country: str,
        table_name: str,
        staging_path: str,
        meta: dict,
        tag: str,
        table_suffix: str | None,
    ) -> None:
        """DELETE changed periods + INSERT from pre-staged parquet."""
        target = self.writer.resolve_table_name(table_name, country, table_suffix=table_suffix)
        l1_col = meta.get("level1_column", "periodo")
        periods = meta.get("periods_to_delete", [])

        logger.info(f"Phase 2 diff_write: {target}, deleting {len(periods)} period(s)")

        # Ensure schema exists
        settings = self.writer._settings
        catalog = settings.catalog
        schema = country
        self.writer._ensure_schema(catalog, schema)

        # If the table doesn't exist, fall back to CTAS
        if not self.writer.table_exists(target):
            logger.info(f"Phase 2: table {target} doesn't exist, falling back to CTAS")
            self._execute_ctas(country, table_name, staging_path, tag, table_suffix)
            return

        # Fix VOID columns if present (from old writes before schema fix)
        self._fix_void_schema(target, staging_path)

        # DELETE periods in batches (supports composite level1 columns)
        if periods:
            l1_cols = parse_columns(l1_col)
            batch_size = 500
            for i in range(0, len(periods), batch_size):
                batch = periods[i : i + batch_size]
                where_clause = databricks_where_in(l1_cols, batch)
                delete_sql = f"DELETE FROM {target} WHERE {where_clause}"
                self.dbx_client.execute_sql(delete_sql)

        # INSERT from staged parquet (CAST to target types)
        target_schema = self.writer._get_table_schema(target)
        if target_schema:
            cols = [c for c in target_schema if c.lower() != "_rescued_data"]
            col_list = ", ".join(f"`{c}`" for c in cols)
            cast_list = ", ".join(
                f"CAST(`{c}` AS {target_schema[c]}) AS `{c}`" for c in cols
            )
            insert_sql = (
                f"INSERT INTO {target} ({col_list}) "
                f"SELECT {cast_list} FROM read_files('{staging_path}', format => 'parquet')"
            )
        else:
            insert_sql = (
                f"INSERT INTO {target} "
                f"SELECT * EXCEPT(_rescued_data) "
                f"FROM read_files('{staging_path}', format => 'parquet')"
            )

        try:
            self.dbx_client.execute_sql(insert_sql)
        except RuntimeError as e:
            err_msg = str(e)
            if any(code in err_msg for code in [
                "DATATYPE_MISMATCH", "CAST_WITHOUT_SUGGESTION",
                "DELTA_DUPLICATE_COLUMNS_FOUND", "UNRESOLVED_COLUMN",
            ]):
                # NEVER fall back to CTAS on diff_write — it would replace
                # the entire table with just the changed periods, destroying
                # historical data.  Log the error and raise so it gets
                # recorded as a failure instead.
                logger.error(
                    f"Phase 2 diff_write INSERT failed (schema mismatch), "
                    f"NOT falling back to CTAS to protect existing data: {e}"
                )
                raise
            raise

        self.writer._apply_tags(target, tag)
        self._cleanup_staging(staging_path)

    def _execute_save_fingerprints(
        self,
        country: str,
        table_name: str,
        meta: dict,
        job_id: str,
    ) -> None:
        """Save fingerprints to Databricks Delta and update local SQLite cache."""
        level = meta.get("level", "period")
        fp_dicts = meta.get("fingerprints", [])
        fingerprints = [
            Fingerprint(
                value=d["value"],
                row_count=d["row_count"],
                checksum_xor=d["checksum_xor"],
            )
            for d in fp_dicts
        ]
        save_fingerprints(
            self.dbx_client,
            self.fingerprint_table,
            country,
            table_name,
            level=level,
            fingerprints=fingerprints,
            job_id=job_id,
        )
        # Update local SQLite cache only after Databricks save succeeds
        self.fingerprint_cache.save(country, table_name, level, fingerprints, job_id)

    def _cleanup_staging(self, staging_dir: str) -> None:
        """Best-effort cleanup of staged parquet files in Volume."""
        try:
            files = self.writer.client.list_files(staging_dir)
            for f in files:
                try:
                    self.writer.client.delete_file(f.path)
                except Exception:
                    pass
        except Exception:
            pass
