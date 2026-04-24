"""Unit tests for two-phase sync components.

Tests FingerprintCache, SyncQueue, Phase2Executor, and the two-phase
integration in diff_sync.
"""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch, call

import polars as pl
import pytest

from sql_databricks_bridge.core.fingerprint import Fingerprint
from sql_databricks_bridge.core.fingerprint_cache import FingerprintCache
from sql_databricks_bridge.core.sync_queue import SyncQueue


# ---------------------------------------------------------------------------
# FingerprintCache
# ---------------------------------------------------------------------------


class TestFingerprintCache:
    """Tests for the local SQLite fingerprint cache."""

    @pytest.fixture
    def cache(self, tmp_path):
        db_path = str(tmp_path / "test_fp_cache.db")
        return FingerprintCache(db_path=db_path)

    def test_empty_cache_returns_no_fingerprints(self, cache):
        result = cache.load("bolivia", "j_atoscompra_new", "period")
        assert result == []

    def test_is_empty_on_fresh_cache(self, cache):
        assert cache.is_empty("bolivia", "j_atoscompra_new", "period")

    def test_save_and_load(self, cache):
        fps = [
            Fingerprint(value="202401", row_count=1000, checksum_xor=12345),
            Fingerprint(value="202402", row_count=2000, checksum_xor=67890),
        ]
        cache.save("bolivia", "j_atoscompra_new", "period", fps, job_id="job-1")

        loaded = cache.load("bolivia", "j_atoscompra_new", "period")
        assert len(loaded) == 2
        assert {fp.value for fp in loaded} == {"202401", "202402"}
        assert not cache.is_empty("bolivia", "j_atoscompra_new", "period")

    def test_save_replaces_existing(self, cache):
        fps_v1 = [Fingerprint(value="202401", row_count=1000, checksum_xor=111)]
        cache.save("bolivia", "table_a", "period", fps_v1, job_id="j1")

        fps_v2 = [
            Fingerprint(value="202401", row_count=1100, checksum_xor=222),
            Fingerprint(value="202402", row_count=500, checksum_xor=333),
        ]
        cache.save("bolivia", "table_a", "period", fps_v2, job_id="j2")

        loaded = cache.load("bolivia", "table_a", "period")
        assert len(loaded) == 2
        assert loaded[0].row_count != 1000 or loaded[1].row_count != 1000

    def test_save_empty_clears_scope(self, cache):
        fps = [Fingerprint(value="202401", row_count=100, checksum_xor=1)]
        cache.save("bolivia", "table_a", "period", fps, job_id="j1")
        assert not cache.is_empty("bolivia", "table_a", "period")

        cache.save("bolivia", "table_a", "period", [], job_id="j2")
        assert cache.is_empty("bolivia", "table_a", "period")

    def test_isolation_between_countries(self, cache):
        fps_bo = [Fingerprint(value="202401", row_count=100, checksum_xor=1)]
        fps_br = [Fingerprint(value="202401", row_count=200, checksum_xor=2)]
        cache.save("bolivia", "table_a", "period", fps_bo, job_id="j1")
        cache.save("brasil", "table_a", "period", fps_br, job_id="j2")

        loaded_bo = cache.load("bolivia", "table_a", "period")
        loaded_br = cache.load("brasil", "table_a", "period")
        assert len(loaded_bo) == 1
        assert loaded_bo[0].row_count == 100
        assert len(loaded_br) == 1
        assert loaded_br[0].row_count == 200

    def test_is_empty_broad_check_with_empty_table_name(self, cache):
        """is_empty(country, '', 'period') should check any table for that country."""
        fps = [Fingerprint(value="202401", row_count=100, checksum_xor=1)]
        cache.save("bolivia", "j_atoscompra_new", "period", fps, job_id="j1")

        # Broad check: any fingerprints for bolivia?
        assert not cache.is_empty("bolivia", "", "period")
        # Specific check: this table has data
        assert not cache.is_empty("bolivia", "j_atoscompra_new", "period")
        # Different country: empty
        assert cache.is_empty("brasil", "", "period")
        # Different table: empty
        assert cache.is_empty("bolivia", "other_table", "period")

    def test_sync_from_databricks(self, cache):
        mock_dbx = MagicMock()
        mock_dbx.execute_sql.return_value = [
            {
                "country": "bolivia",
                "table_name": "j_atoscompra_new",
                "level": "period",
                "level1_value": "202401",
                "row_count": 5000,
                "checksum_xor": 99999,
                "synced_at": "2024-01-15 10:00:00",
                "job_id": "remote-job-1",
            },
            {
                "country": "bolivia",
                "table_name": "j_atoscompra_new",
                "level": "period",
                "level1_value": "202402",
                "row_count": 3000,
                "checksum_xor": 88888,
                "synced_at": "2024-01-15 10:00:00",
                "job_id": "remote-job-1",
            },
        ]

        count = cache.sync_from_databricks(mock_dbx, "bridge.events.fingerprints", country="bolivia")
        assert count == 2

        loaded = cache.load("bolivia", "j_atoscompra_new", "period")
        assert len(loaded) == 2

    def test_sync_from_databricks_handles_failure(self, cache):
        mock_dbx = MagicMock()
        mock_dbx.execute_sql.side_effect = RuntimeError("Warehouse not running")

        count = cache.sync_from_databricks(mock_dbx, "bridge.events.fingerprints")
        assert count == 0


# ---------------------------------------------------------------------------
# SyncQueue
# ---------------------------------------------------------------------------


class TestSyncQueue:
    """Tests for the persistent sync queue."""

    @pytest.fixture
    def queue(self, tmp_path):
        db_path = str(tmp_path / "test_sync_queue.db")
        return SyncQueue(db_path=db_path)

    def test_enqueue_and_get_pending(self, queue):
        qid = queue.enqueue(
            job_id="job-1",
            country="bolivia",
            table_name="j_atoscompra_new",
            operation="diff_write",
            staging_path="/Volumes/main/bolivia/vol/_staging_2p/j_atoscompra_new_job-1",
            metadata={"periods_to_delete": ["202401", "202402"], "level1_column": "periodo"},
            tag="v_202401",
            table_suffix="",
        )
        assert qid is not None

        pending = queue.get_pending(job_id="job-1")
        assert len(pending) == 1
        assert pending[0]["operation"] == "diff_write"
        assert pending[0]["metadata"]["periods_to_delete"] == ["202401", "202402"]

    def test_multiple_items_ordered_by_id(self, queue):
        queue.enqueue(job_id="j1", country="bo", table_name="t1", operation="ctas")
        queue.enqueue(job_id="j1", country="bo", table_name="t2", operation="diff_write")
        queue.enqueue(job_id="j1", country="bo", table_name="t1", operation="save_fingerprints")

        pending = queue.get_pending(job_id="j1")
        assert len(pending) == 3
        assert pending[0]["table_name"] == "t1"
        assert pending[1]["table_name"] == "t2"
        assert pending[2]["operation"] == "save_fingerprints"

    def test_mark_committed(self, queue):
        qid = queue.enqueue(job_id="j1", country="bo", table_name="t1", operation="ctas")
        queue.mark_committed(qid)

        pending = queue.get_pending(job_id="j1")
        assert len(pending) == 0

    def test_mark_failed(self, queue):
        qid = queue.enqueue(job_id="j1", country="bo", table_name="t1", operation="ctas")
        queue.mark_failed(qid, "Some error occurred")

        pending = queue.get_pending(job_id="j1")
        assert len(pending) == 0

    def test_count_pending(self, queue):
        queue.enqueue(job_id="j1", country="bo", table_name="t1", operation="ctas")
        queue.enqueue(job_id="j1", country="bo", table_name="t2", operation="ctas")
        queue.enqueue(job_id="j2", country="br", table_name="t1", operation="ctas")

        assert queue.count_pending() == 3
        assert queue.count_pending(job_id="j1") == 2
        assert queue.count_pending(job_id="j2") == 1

    def test_filter_by_job_id(self, queue):
        queue.enqueue(job_id="j1", country="bo", table_name="t1", operation="ctas")
        queue.enqueue(job_id="j2", country="br", table_name="t1", operation="ctas")

        pending_j1 = queue.get_pending(job_id="j1")
        pending_j2 = queue.get_pending(job_id="j2")
        assert len(pending_j1) == 1
        assert len(pending_j2) == 1

    def test_cleanup_old(self, queue):
        import sqlite3
        # Enqueue and commit an item
        qid = queue.enqueue(job_id="j1", country="bo", table_name="t1", operation="ctas")
        queue.mark_committed(qid)

        # Manually backdate the created_at so cleanup finds it
        conn = sqlite3.connect(queue.db_path)
        conn.execute(
            "UPDATE sync_queue SET created_at = '2020-01-01 00:00:00' WHERE id = ?",
            (qid,),
        )
        conn.commit()
        conn.close()

        removed = queue.cleanup_old(days=7)
        assert removed == 1


# ---------------------------------------------------------------------------
# Phase2Executor
# ---------------------------------------------------------------------------


class TestPhase2Executor:
    """Tests for the Phase 2 batch executor."""

    @pytest.fixture
    def queue(self, tmp_path):
        return SyncQueue(db_path=str(tmp_path / "p2_queue.db"))

    @pytest.fixture
    def fp_cache(self, tmp_path):
        return FingerprintCache(db_path=str(tmp_path / "p2_cache.db"))

    @pytest.fixture
    def mock_dbx(self):
        mock = MagicMock()
        mock.execute_sql.return_value = []
        mock.list_files.return_value = []
        return mock

    @pytest.fixture
    def mock_writer(self, mock_dbx):
        mock = MagicMock()
        mock.client = mock_dbx
        mock.resolve_table_name.return_value = "`main`.`bolivia`.`j_atoscompra_new`"
        mock.table_exists.return_value = True
        mock._get_table_columns.return_value = ["periodo", "idproduto", "cantidad"]
        mock._get_table_schema.return_value = {
            "periodo": "INT", "idproduto": "STRING", "cantidad": "DOUBLE",
        }
        mock._settings = MagicMock()
        mock._settings.catalog = "main"
        return mock

    def test_execute_batch_empty_queue(self, mock_dbx, mock_writer, queue, fp_cache):
        from sql_databricks_bridge.core.phase2_executor import Phase2Executor

        executor = Phase2Executor(
            dbx_client=mock_dbx,
            writer=mock_writer,
            queue=queue,
            fingerprint_cache=fp_cache,
            fingerprint_table="bridge.events.fingerprints",
        )
        result = executor.execute_batch(job_id="j1")
        assert result.tables_committed == 0
        assert result.tables_failed == 0

    def test_execute_ctas_item(self, mock_dbx, mock_writer, queue, fp_cache):
        from sql_databricks_bridge.core.phase2_executor import Phase2Executor

        queue.enqueue(
            job_id="j1", country="bolivia", table_name="table_a",
            operation="ctas",
            staging_path="/Volumes/main/bolivia/vol/_staging_2p/table_a_j1",
        )

        executor = Phase2Executor(
            dbx_client=mock_dbx,
            writer=mock_writer,
            queue=queue,
            fingerprint_cache=fp_cache,
            fingerprint_table="bridge.events.fingerprints",
        )
        result = executor.execute_batch(job_id="j1")

        assert result.tables_committed == 1
        assert result.tables_failed == 0
        # Verify INSERT OVERWRITE was executed (table_exists=True)
        assert mock_dbx.execute_sql.called
        overwrite_calls = [
            c for c in mock_dbx.execute_sql.call_args_list
            if "INSERT OVERWRITE" in str(c)
        ]
        assert len(overwrite_calls) >= 1

    def test_execute_diff_write_item(self, mock_dbx, mock_writer, queue, fp_cache):
        from sql_databricks_bridge.core.phase2_executor import Phase2Executor

        queue.enqueue(
            job_id="j1", country="bolivia", table_name="j_atoscompra_new",
            operation="diff_write",
            staging_path="/Volumes/main/bolivia/vol/_staging_2p/j_atoscompra_new_j1",
            metadata={
                "periods_to_delete": ["202401", "202402"],
                "level1_column": "periodo",
                "level2_column": "idproduto",
            },
        )

        executor = Phase2Executor(
            dbx_client=mock_dbx,
            writer=mock_writer,
            queue=queue,
            fingerprint_cache=fp_cache,
            fingerprint_table="bridge.events.fingerprints",
        )
        result = executor.execute_batch(job_id="j1")

        assert result.tables_committed == 1
        # Verify DELETE was executed
        delete_calls = [
            c for c in mock_dbx.execute_sql.call_args_list
            if "DELETE FROM" in str(c)
        ]
        assert len(delete_calls) >= 1

    def test_execute_save_fingerprints_item(self, mock_dbx, mock_writer, queue, fp_cache):
        from sql_databricks_bridge.core.phase2_executor import Phase2Executor

        queue.enqueue(
            job_id="j1", country="bolivia", table_name="j_atoscompra_new",
            operation="save_fingerprints",
            metadata={
                "level": "period",
                "fingerprints": [
                    {"value": "202401", "row_count": 1000, "checksum_xor": 12345},
                    {"value": "202402", "row_count": 2000, "checksum_xor": 67890},
                ],
            },
        )

        executor = Phase2Executor(
            dbx_client=mock_dbx,
            writer=mock_writer,
            queue=queue,
            fingerprint_cache=fp_cache,
            fingerprint_table="bridge.events.fingerprints",
        )
        result = executor.execute_batch(job_id="j1")

        assert result.fingerprints_saved == 1
        # Verify fingerprints were saved to Databricks
        fp_calls = [
            c for c in mock_dbx.execute_sql.call_args_list
            if "INSERT INTO" in str(c) and "fingerprint" in str(c).lower()
        ]
        assert len(fp_calls) >= 1

    def test_failed_item_marked_in_queue(self, mock_dbx, mock_writer, queue, fp_cache):
        from sql_databricks_bridge.core.phase2_executor import Phase2Executor

        # Make CTAS fail
        mock_dbx.execute_sql.side_effect = RuntimeError("Warehouse timeout")

        queue.enqueue(
            job_id="j1", country="bolivia", table_name="table_a",
            operation="ctas",
            staging_path="/Volumes/main/bolivia/vol/_staging_2p/table_a_j1",
        )

        executor = Phase2Executor(
            dbx_client=mock_dbx,
            writer=mock_writer,
            queue=queue,
            fingerprint_cache=fp_cache,
            fingerprint_table="bridge.events.fingerprints",
        )
        result = executor.execute_batch(job_id="j1")

        assert result.tables_failed == 1
        assert result.tables_committed == 0
        assert len(result.errors) == 1
        # Queue item should be marked as failed, not pending
        assert queue.count_pending(job_id="j1") == 0

    def test_mixed_batch(self, mock_dbx, mock_writer, queue, fp_cache):
        from sql_databricks_bridge.core.phase2_executor import Phase2Executor

        queue.enqueue(
            job_id="j1", country="bolivia", table_name="t1",
            operation="ctas",
            staging_path="/Volumes/main/bolivia/vol/_staging_2p/t1_j1",
        )
        queue.enqueue(
            job_id="j1", country="bolivia", table_name="t2",
            operation="diff_write",
            staging_path="/Volumes/main/bolivia/vol/_staging_2p/t2_j1",
            metadata={"periods_to_delete": ["202401"], "level1_column": "periodo"},
        )
        queue.enqueue(
            job_id="j1", country="bolivia", table_name="t1",
            operation="save_fingerprints",
            metadata={"level": "period", "fingerprints": [
                {"value": "202401", "row_count": 100, "checksum_xor": 1},
            ]},
        )
        queue.enqueue(
            job_id="j1", country="bolivia", table_name="t2",
            operation="save_fingerprints",
            metadata={"level": "period", "fingerprints": [
                {"value": "202401", "row_count": 200, "checksum_xor": 2},
            ]},
        )

        executor = Phase2Executor(
            dbx_client=mock_dbx,
            writer=mock_writer,
            queue=queue,
            fingerprint_cache=fp_cache,
            fingerprint_table="bridge.events.fingerprints",
        )
        result = executor.execute_batch(job_id="j1")

        assert result.tables_committed == 2
        assert result.fingerprints_saved == 2
        assert result.tables_failed == 0
        assert queue.count_pending(job_id="j1") == 0


# ---------------------------------------------------------------------------
# DiffSync two-phase integration
# ---------------------------------------------------------------------------


class TestDiffSyncTwoPhase:
    """Test that diff_sync correctly enqueues instead of writing in two-phase mode."""

    @pytest.fixture
    def fp_cache(self, tmp_path):
        return FingerprintCache(db_path=str(tmp_path / "ds_cache.db"))

    @pytest.fixture
    def sync_queue(self, tmp_path):
        return SyncQueue(db_path=str(tmp_path / "ds_queue.db"))

    @patch("sql_databricks_bridge.core.config.get_settings")
    def test_two_phase_enqueues_diff_write(
        self, mock_get_settings, fp_cache, sync_queue, tmp_path,
    ):
        """In two-phase mode, diff_sync should enqueue rather than write to Databricks."""
        from sql_databricks_bridge.core.diff_sync import run_differential_sync

        # Setup mock settings
        mock_settings = MagicMock()
        mock_settings.databricks.catalog = "main"
        mock_settings.databricks.volume = "landing"
        mock_get_settings.return_value = mock_settings

        # Mock SQL client: return fingerprints showing changes
        mock_sql = MagicMock()
        mock_sql.execute_query.return_value = pl.DataFrame({
            "grp_value": ["202401", "202402"],
            "cnt": [1000, 2000],
            "chk": [111, 222],
        })
        # Mock disk extraction: return a temp parquet file
        _tmp_dir = tmp_path / "chunks" / "diff_test-job-1" / "j_atoscompra_new" / "bulk"
        _tmp_dir.mkdir(parents=True, exist_ok=True)
        _part_file = _tmp_dir / "part_0.parquet"
        _sample_df = pl.DataFrame({
            "periodo": ["202401"] * 500 + ["202402"] * 500,
            "idproduto": list(range(1000)),
            "cantidad": [10] * 1000,
        })
        _sample_df.write_parquet(_part_file)
        mock_sql.execute_query_to_disk.return_value = (
            [_part_file],
            1000,
        )

        # Seed the cache with old fingerprints (different checksum → triggers extraction)
        fp_cache.save(
            "bolivia", "j_atoscompra_new", "period",
            [Fingerprint(value="202401", row_count=900, checksum_xor=999)],
            job_id="old",
        )

        mock_dbx = MagicMock()
        mock_dbx.execute_sql.return_value = []
        mock_dbx.upload_dataframe_chunked.return_value = []

        mock_writer = MagicMock()
        mock_writer.resolve_table_name.return_value = "`main`.`bolivia`.`j_atoscompra_new`"
        mock_writer.client = mock_dbx
        mock_writer._settings = mock_settings.databricks

        stats = run_differential_sync(
            sql_client=mock_sql,
            dbx_client=mock_dbx,
            writer=mock_writer,
            sql_table="j_atoscompra_new",
            country="bolivia",
            level1_column="periodo",
            level2_column="idproduto",
            fingerprint_table="bridge.events.fingerprints",
            job_id="test-job-1",
            two_phase=True,
            fingerprint_cache=fp_cache,
            sync_queue=sync_queue,
        )

        # Should have queued items
        assert stats.queued is True

        # Check queue has items
        pending = sync_queue.get_pending(job_id="test-job-1")
        assert len(pending) >= 1

        # Should NOT have called ensure_fingerprint_table (requires warehouse)
        ensure_calls = [
            c for c in mock_dbx.execute_sql.call_args_list
            if "CREATE TABLE IF NOT EXISTS" in str(c)
        ]
        assert len(ensure_calls) == 0

    def test_two_phase_no_changes_enqueues_fingerprint_save(self, fp_cache, sync_queue):
        """When no changes are detected, still enqueue fingerprint save to keep Delta in sync."""
        from sql_databricks_bridge.core.diff_sync import run_differential_sync

        mock_sql = MagicMock()
        # Return same fingerprint as cached
        mock_sql.execute_query.return_value = pl.DataFrame({
            "grp_value": ["202401"],
            "cnt": [1000],
            "chk": [111],
        })

        # Seed cache with same fingerprint
        fp_cache.save(
            "bolivia", "table_a", "period",
            [Fingerprint(value="202401", row_count=1000, checksum_xor=111)],
            job_id="old",
        )

        mock_dbx = MagicMock()
        mock_writer = MagicMock()

        stats = run_differential_sync(
            sql_client=mock_sql,
            dbx_client=mock_dbx,
            writer=mock_writer,
            sql_table="table_a",
            country="bolivia",
            level1_column="periodo",
            level2_column="idproduto",
            fingerprint_table="bridge.events.fingerprints",
            job_id="test-job-2",
            two_phase=True,
            fingerprint_cache=fp_cache,
            sync_queue=sync_queue,
        )

        # No data changes, so no write queued
        assert stats.rows_downloaded == 0
        assert stats.unchanged_level1 == 1

        # But fingerprint save should be enqueued to keep Delta in sync
        pending = sync_queue.get_pending(job_id="test-job-2")
        fp_items = [p for p in pending if p["operation"] == "save_fingerprints"]
        assert len(fp_items) == 1


# ---------------------------------------------------------------------------
# VOID column fix tests
# ---------------------------------------------------------------------------


class TestVoidColumnFix:
    """Test that Phase 2 detects and fixes VOID columns in Delta tables."""

    @pytest.fixture
    def fp_cache(self, tmp_path):
        return FingerprintCache(db_path=str(tmp_path / "void_cache.db"))

    @pytest.fixture
    def queue(self, tmp_path):
        return SyncQueue(db_path=str(tmp_path / "void_queue.db"))

    @pytest.fixture
    def mock_dbx(self):
        mock = MagicMock()
        mock.execute_sql.return_value = []
        mock.list_files.return_value = []
        return mock

    @pytest.fixture
    def mock_writer(self, mock_dbx):
        mock = MagicMock()
        mock.client = mock_dbx
        mock.resolve_table_name.return_value = "`main`.`bolivia`.`my_table`"
        mock.table_exists.return_value = True
        mock._settings = MagicMock()
        mock._settings.catalog = "main"
        return mock

    def test_void_detected_and_fixed_in_ctas(self, mock_dbx, mock_writer, queue, fp_cache):
        """CTAS path: VOID columns trigger backup-recreate-copy flow."""
        from sql_databricks_bridge.core.phase2_executor import Phase2Executor

        # First call to _get_table_schema returns VOID, second returns fixed schema
        mock_writer._get_table_schema.side_effect = [
            # 1st: _fix_void_schema detects VOID
            {"periodo": "INT", "cantidad": "VOID", "nombre": "STRING"},
            # 2nd: _fix_void_schema reads __old schema
            {"periodo": "INT", "cantidad": "VOID", "nombre": "STRING"},
            # 3rd: _fix_void_schema reads new table schema (from parquet)
            {"periodo": "INT", "cantidad": "DOUBLE", "nombre": "STRING"},
            # 4th: _execute_ctas reads schema for INSERT OVERWRITE TRY_CAST list
            {"periodo": "INT", "cantidad": "DOUBLE", "nombre": "STRING"},
        ]

        queue.enqueue(
            job_id="j1", country="bolivia", table_name="my_table",
            operation="ctas",
            staging_path="/Volumes/main/bolivia/vol/_staging/my_table_j1",
        )

        executor = Phase2Executor(
            dbx_client=mock_dbx, writer=mock_writer, queue=queue,
            fingerprint_cache=fp_cache, fingerprint_table="bridge.events.fingerprints",
        )
        result = executor.execute_batch(job_id="j1")

        assert result.tables_committed == 1
        assert result.tables_failed == 0

        # Verify the VOID fix SQL sequence was executed
        sql_calls = [str(c) for c in mock_dbx.execute_sql.call_args_list]
        # Should have: ALTER TABLE RENAME, CREATE TABLE ... WHERE 1=0, INSERT INTO (copy), DROP TABLE __old
        assert any("RENAME TO" in s for s in sql_calls), f"No RENAME in: {sql_calls}"
        assert any("WHERE 1=0" in s for s in sql_calls), f"No empty CREATE in: {sql_calls}"
        assert any("__old" in s and "DROP TABLE" in s for s in sql_calls), f"No DROP __old in: {sql_calls}"
        # And finally the INSERT OVERWRITE with the new data
        assert any("INSERT OVERWRITE" in s for s in sql_calls), f"No INSERT OVERWRITE in: {sql_calls}"

    def test_no_void_skips_fix(self, mock_dbx, mock_writer, queue, fp_cache):
        """When no VOID columns, _fix_void_schema returns False and no backup happens."""
        from sql_databricks_bridge.core.phase2_executor import Phase2Executor

        mock_writer._get_table_schema.return_value = {
            "periodo": "INT", "cantidad": "DOUBLE", "nombre": "STRING",
        }

        queue.enqueue(
            job_id="j1", country="bolivia", table_name="my_table",
            operation="ctas",
            staging_path="/Volumes/main/bolivia/vol/_staging/my_table_j1",
        )

        executor = Phase2Executor(
            dbx_client=mock_dbx, writer=mock_writer, queue=queue,
            fingerprint_cache=fp_cache, fingerprint_table="bridge.events.fingerprints",
        )
        result = executor.execute_batch(job_id="j1")

        assert result.tables_committed == 1
        sql_calls = [str(c) for c in mock_dbx.execute_sql.call_args_list]
        # Should NOT have any RENAME (no VOID fix needed)
        assert not any("RENAME TO" in s for s in sql_calls), f"Unexpected RENAME in: {sql_calls}"
        # Should still do INSERT OVERWRITE
        assert any("INSERT OVERWRITE" in s for s in sql_calls)

    def test_void_detected_in_diff_write(self, mock_dbx, mock_writer, queue, fp_cache):
        """diff_write path: VOID columns get fixed before DELETE+INSERT."""
        from sql_databricks_bridge.core.phase2_executor import Phase2Executor

        mock_writer._get_table_schema.side_effect = [
            # 1st: _fix_void_schema detects VOID
            {"periodo": "INT", "cantidad": "VOID"},
            # 2nd: _fix_void_schema reads __old schema
            {"periodo": "INT", "cantidad": "VOID"},
            # 3rd: _fix_void_schema reads new table schema
            {"periodo": "INT", "cantidad": "DOUBLE"},
            # 4th: _execute_diff_write reads schema for INSERT CAST
            {"periodo": "INT", "cantidad": "DOUBLE"},
        ]

        queue.enqueue(
            job_id="j1", country="bolivia", table_name="my_table",
            operation="diff_write",
            staging_path="/Volumes/main/bolivia/vol/_staging/my_table_j1",
            metadata={
                "periods_to_delete": ["202401"],
                "level1_column": "periodo",
            },
        )

        executor = Phase2Executor(
            dbx_client=mock_dbx, writer=mock_writer, queue=queue,
            fingerprint_cache=fp_cache, fingerprint_table="bridge.events.fingerprints",
        )
        result = executor.execute_batch(job_id="j1")

        assert result.tables_committed == 1
        assert result.tables_failed == 0

        sql_calls = [str(c) for c in mock_dbx.execute_sql.call_args_list]
        # VOID fix happened
        assert any("RENAME TO" in s for s in sql_calls)
        assert any("DROP TABLE" in s and "__old" in s for s in sql_calls)
        # Normal diff_write DELETE + INSERT happened after fix
        assert any("DELETE FROM" in s for s in sql_calls)
        assert any("INSERT INTO" in s for s in sql_calls)

    def test_void_fix_recovery_on_failure(self, mock_dbx, mock_writer, queue, fp_cache):
        """If VOID fix fails mid-way, __old is renamed back."""
        from sql_databricks_bridge.core.phase2_executor import Phase2Executor

        mock_writer._get_table_schema.side_effect = [
            # 1st: detects VOID
            {"periodo": "INT", "cantidad": "VOID"},
        ]

        # RENAME succeeds, CREATE fails
        call_count = 0
        def side_effect(sql):
            nonlocal call_count
            call_count += 1
            if "ALTER TABLE" in sql and "RENAME TO" in sql:
                return []  # RENAME succeeds
            if "CREATE TABLE" in sql:
                raise RuntimeError("Warehouse timeout")
            if "DROP TABLE IF EXISTS" in sql:
                return []  # recovery DROP succeeds
            return []

        mock_dbx.execute_sql.side_effect = side_effect

        queue.enqueue(
            job_id="j1", country="bolivia", table_name="my_table",
            operation="ctas",
            staging_path="/Volumes/main/bolivia/vol/_staging/my_table_j1",
        )

        executor = Phase2Executor(
            dbx_client=mock_dbx, writer=mock_writer, queue=queue,
            fingerprint_cache=fp_cache, fingerprint_table="bridge.events.fingerprints",
        )
        result = executor.execute_batch(job_id="j1")

        # Should have failed
        assert result.tables_failed == 1
        assert result.tables_committed == 0

        # Recovery: should have tried DROP IF EXISTS + ALTER TABLE RENAME back
        sql_calls = [str(c) for c in mock_dbx.execute_sql.call_args_list]
        assert any("DROP TABLE IF EXISTS" in s for s in sql_calls)
        # The second ALTER TABLE RENAME restores __old back
        rename_calls = [s for s in sql_calls if "RENAME TO" in s]
        assert len(rename_calls) >= 2  # original rename + recovery rename


# ---------------------------------------------------------------------------
# Fingerprint timing: cache only saved after Phase 2
# ---------------------------------------------------------------------------


class TestFingerprintTiming:
    """Verify fingerprints are NOT saved to local cache during Phase 1,
    and ARE saved during Phase 2."""

    @pytest.fixture
    def fp_cache(self, tmp_path):
        return FingerprintCache(db_path=str(tmp_path / "timing_cache.db"))

    @pytest.fixture
    def sync_queue(self, tmp_path):
        return SyncQueue(db_path=str(tmp_path / "timing_queue.db"))

    @patch("sql_databricks_bridge.core.config.get_settings")
    def test_phase1_does_not_save_cache(
        self, mock_get_settings, fp_cache, sync_queue, tmp_path,
    ):
        """Phase 1 (diff_sync with two_phase=True) should NOT save to local cache."""
        from sql_databricks_bridge.core.diff_sync import run_differential_sync

        mock_settings = MagicMock()
        mock_settings.databricks.catalog = "main"
        mock_settings.databricks.volume = "landing"
        mock_get_settings.return_value = mock_settings

        mock_sql = MagicMock()
        # Return fingerprint with change
        mock_sql.execute_query.return_value = pl.DataFrame({
            "grp_value": ["202401"],
            "cnt": [1000],
            "chk": [111],
        })
        _tmp_dir = tmp_path / "chunks" / "diff_tp-j1" / "t1" / "bulk"
        _tmp_dir.mkdir(parents=True, exist_ok=True)
        _part = _tmp_dir / "part_0.parquet"
        pl.DataFrame({"periodo": ["202401"] * 100, "val": list(range(100))}).write_parquet(_part)
        mock_sql.execute_query_to_disk.return_value = ([_part], 100)

        # Seed cache with different fingerprint to trigger extraction
        fp_cache.save("bolivia", "t1", "period",
                      [Fingerprint(value="202401", row_count=500, checksum_xor=999)], "old")

        mock_dbx = MagicMock()
        mock_dbx.execute_sql.return_value = []
        mock_writer = MagicMock()
        mock_writer.client = mock_dbx
        mock_writer._settings = mock_settings.databricks
        mock_writer.resolve_table_name.return_value = "`main`.`bolivia`.`t1`"

        stats = run_differential_sync(
            sql_client=mock_sql, dbx_client=mock_dbx, writer=mock_writer,
            sql_table="t1", country="bolivia",
            level1_column="periodo", level2_column="idproduto",
            fingerprint_table="bridge.events.fingerprints",
            job_id="tp-j1",
            two_phase=True, fingerprint_cache=fp_cache, sync_queue=sync_queue,
        )
        assert stats.queued is True

        # Local cache should still have the OLD fingerprint (checksum_xor=999),
        # NOT the new one (111), because Phase 1 should not update the cache.
        cached = fp_cache.load("bolivia", "t1", "period")
        assert len(cached) == 1
        assert cached[0].checksum_xor == 999, (
            f"Cache was updated prematurely! Expected old xor=999, got {cached[0].checksum_xor}"
        )

    def test_phase2_saves_cache(self, fp_cache, sync_queue):
        """Phase 2 (_execute_save_fingerprints) SHOULD save to local cache."""
        from sql_databricks_bridge.core.phase2_executor import Phase2Executor

        mock_dbx = MagicMock()
        mock_dbx.execute_sql.return_value = []
        mock_writer = MagicMock()
        mock_writer._get_table_schema.return_value = {}
        mock_writer._settings = MagicMock()
        mock_writer._settings.catalog = "main"

        sync_queue.enqueue(
            job_id="j1", country="bolivia", table_name="t1",
            operation="save_fingerprints",
            metadata={
                "level": "period",
                "fingerprints": [
                    {"value": "202401", "row_count": 1000, "checksum_xor": 111},
                    {"value": "202402", "row_count": 2000, "checksum_xor": 222},
                ],
            },
        )

        executor = Phase2Executor(
            dbx_client=mock_dbx, writer=mock_writer, queue=sync_queue,
            fingerprint_cache=fp_cache, fingerprint_table="bridge.events.fingerprints",
        )
        result = executor.execute_batch(job_id="j1")

        assert result.fingerprints_saved == 1

        # Local cache should now have the fingerprints
        cached = fp_cache.load("bolivia", "t1", "period")
        assert len(cached) == 2
        values = {fp.value: fp.checksum_xor for fp in cached}
        assert values["202401"] == 111
        assert values["202402"] == 222

    @patch("sql_databricks_bridge.core.config.get_settings")
    def test_fingerprint_only_mode_no_premature_cache(
        self, mock_get_settings, fp_cache, sync_queue,
    ):
        """fingerprint_only=True in two-phase should NOT save to local cache."""
        from sql_databricks_bridge.core.diff_sync import run_differential_sync

        mock_settings = MagicMock()
        mock_settings.databricks.catalog = "main"
        mock_get_settings.return_value = mock_settings

        mock_sql = MagicMock()
        mock_sql.execute_query.return_value = pl.DataFrame({
            "grp_value": ["202401"],
            "cnt": [500],
            "chk": [777],
        })

        mock_dbx = MagicMock()
        mock_writer = MagicMock()

        stats = run_differential_sync(
            sql_client=mock_sql, dbx_client=mock_dbx, writer=mock_writer,
            sql_table="dim_table", country="bolivia",
            level1_column="periodo", level2_column="",
            fingerprint_table="bridge.events.fingerprints",
            job_id="fp-only-1",
            fingerprint_only=True,
            two_phase=True, fingerprint_cache=fp_cache, sync_queue=sync_queue,
        )
        assert stats.queued is True

        # Local cache should be empty — Phase 1 should NOT have saved
        cached = fp_cache.load("bolivia", "dim_table", "period")
        assert len(cached) == 0, (
            f"Cache should be empty but has {len(cached)} entries (premature save!)"
        )
