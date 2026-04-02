"""Unit tests for diff_sync module — focused on the table-not-exists + fingerprints bug."""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from sql_databricks_bridge.core.diff_sync import DiffSyncStats, run_differential_sync
from sql_databricks_bridge.core.fingerprint import Fingerprint


@pytest.fixture
def mock_sql_client():
    """Mock SQL Server client that returns fingerprints and data."""
    client = MagicMock()
    # Default: return 3 sectors with fingerprints
    client.execute_query.return_value = pl.DataFrame({
        "grp_value": ["10", "20", "50"],
        "cnt": [100, 200, 50],
        "chk": [111, 222, 333],
    })
    return client


@pytest.fixture
def mock_dbx_client():
    """Mock Databricks client."""
    client = MagicMock()
    client.execute_sql.return_value = []
    return client


@pytest.fixture
def mock_writer():
    """Mock DeltaTableWriter."""
    writer = MagicMock()
    writer.resolve_table_name.return_value = "`cat`.`colombia`.`vw_artigoz_full`"
    writer._get_table_columns.return_value = []
    from sql_databricks_bridge.core.delta_writer import WriteResult
    writer.write_dataframe.return_value = WriteResult(
        table_name="`cat`.`colombia`.`vw_artigoz_full`",
        rows=350,
        duration_seconds=2.0,
    )
    writer.write_diff_slices.return_value = WriteResult(
        table_name="`cat`.`colombia`.`vw_artigoz_full`",
        rows=100,
        duration_seconds=1.0,
    )
    return writer


class TestTargetTableMissing:
    """Regression test: when target table doesn't exist but fingerprints do,
    diff-sync must force a full extraction (not partial).

    This was the root cause of missing sectors in vw_artigoz_full for Colombia:
    fingerprints from vw_artigoz (no suffix) made diff-sync think sectors were
    "unchanged", so only changed sectors were written to the new _full table.
    """

    def test_missing_target_forces_full_sync(
        self, mock_sql_client, mock_dbx_client, mock_writer
    ):
        """If target table doesn't exist but stored fingerprints match,
        diff-sync should force is_first_sync=True and do full extraction."""

        # Stored fingerprints match current → normally diff-sync would skip
        stored_fingerprints = [
            {"grp_value": "10", "row_count": 100, "checksum_xor": 111},
            {"grp_value": "20", "row_count": 200, "checksum_xor": 222},
            {"grp_value": "50", "row_count": 50, "checksum_xor": 333},
        ]
        mock_dbx_client.execute_sql.return_value = stored_fingerprints

        # Target table does NOT exist
        mock_writer.table_exists.return_value = False

        # SQL extraction returns all rows
        all_data = pl.DataFrame({
            "idsector": [10, 10, 20, 20, 20, 50],
            "idproduto": [1, 2, 3, 4, 5, 6],
            "name": ["a", "b", "c", "d", "e", "f"],
        })
        mock_sql_client.execute_query_to_disk.return_value = ([], 0)

        # Mock the chunked extraction to return all data
        mock_sql_client.execute_query_chunked.return_value = iter([all_data])

        with patch(
            "sql_databricks_bridge.core.diff_sync._extract_with_retry",
            return_value=([all_data], len(all_data)),
        ):
            stats = run_differential_sync(
                sql_client=mock_sql_client,
                dbx_client=mock_dbx_client,
                writer=mock_writer,
                sql_table="vw_artigoz",
                country="colombia",
                level1_column="idsector",
                level2_column="idproduto",
                fingerprint_table="bridge.events.sync_fingerprints",
                table_suffix="_full",
            )

        # Key assertions: it should have treated this as first sync
        assert stats.is_first_sync is True
        # Should have used write_dataframe (OVERWRITE), not write_diff_slices
        mock_writer.write_dataframe.assert_called_once()
        mock_writer.write_diff_slices.assert_not_called()

    def test_existing_target_with_matching_fps_does_not_extract(
        self, mock_sql_client, mock_dbx_client, mock_writer
    ):
        """If target table EXISTS and fingerprints match, no extraction needed."""

        stored_fingerprints = [
            {"grp_value": "10", "row_count": 100, "checksum_xor": 111},
            {"grp_value": "20", "row_count": 200, "checksum_xor": 222},
            {"grp_value": "50", "row_count": 50, "checksum_xor": 333},
        ]
        mock_dbx_client.execute_sql.return_value = stored_fingerprints

        # Target table EXISTS
        mock_writer.table_exists.return_value = True

        stats = run_differential_sync(
            sql_client=mock_sql_client,
            dbx_client=mock_dbx_client,
            writer=mock_writer,
            sql_table="vw_artigoz",
            country="colombia",
            level1_column="idsector",
            level2_column="idproduto",
            fingerprint_table="bridge.events.sync_fingerprints",
            table_suffix="_full",
        )

        # No changes detected, no extraction
        assert stats.is_first_sync is False
        assert stats.changed_level1 == 0
        assert stats.rows_downloaded == 0
        mock_writer.write_dataframe.assert_not_called()
        mock_writer.write_diff_slices.assert_not_called()


class TestSuffixAwareFingerprints:
    """Tests for suffix-aware fingerprint keying.

    When table_suffix is set (e.g. '_full'), fingerprints must be stored/loaded
    under the suffixed name so base and suffixed tables don't collide.
    """

    def test_suffix_uses_independent_fingerprints(
        self, mock_sql_client, mock_dbx_client, mock_writer
    ):
        """With table_suffix='_full', fingerprints should load/save under
        'vw_artigoz_full', not 'vw_artigoz'."""

        # Target table exists — no forced full sync
        mock_writer.table_exists.return_value = True

        # Databricks returns NO stored fingerprints for 'vw_artigoz_full'
        # (but would return some if queried for 'vw_artigoz')
        mock_dbx_client.execute_sql.return_value = []

        all_data = pl.DataFrame({
            "idsector": [10, 10, 20, 20, 20, 50],
            "idproduto": [1, 2, 3, 4, 5, 6],
            "name": ["a", "b", "c", "d", "e", "f"],
        })

        with patch(
            "sql_databricks_bridge.core.diff_sync._extract_with_retry",
            return_value=([all_data], len(all_data)),
        ):
            stats = run_differential_sync(
                sql_client=mock_sql_client,
                dbx_client=mock_dbx_client,
                writer=mock_writer,
                sql_table="vw_artigoz",
                country="colombia",
                level1_column="idsector",
                level2_column="idproduto",
                fingerprint_table="bridge.events.sync_fingerprints",
                table_suffix="_full",
            )

        # Should be first sync (no stored fps for suffixed name)
        assert stats.is_first_sync is True

        # Verify load_stored_fingerprints was called with suffixed name
        load_call_sql = mock_dbx_client.execute_sql.call_args_list[0]
        # The first call is ensure_fingerprint_table (DDL), second is load
        # Find the SELECT call that loads fingerprints
        select_calls = [
            c for c in mock_dbx_client.execute_sql.call_args_list
            if "SELECT" in str(c) and "table_name" in str(c)
        ]
        assert len(select_calls) >= 1
        # The fingerprint load query should reference 'vw_artigoz_full'
        assert "vw_artigoz_full" in str(select_calls[0])

        # Verify save_fingerprints was called with suffixed name
        save_calls = [
            c for c in mock_dbx_client.execute_sql.call_args_list
            if "INSERT INTO" in str(c)
        ]
        assert len(save_calls) >= 1
        assert "vw_artigoz_full" in str(save_calls[0])

    def test_no_suffix_backward_compatible(
        self, mock_sql_client, mock_dbx_client, mock_writer
    ):
        """Without table_suffix, fingerprints use the raw sql_table name."""

        # No suffix: table_suffix=None (default)
        mock_writer.table_exists.return_value = True
        mock_dbx_client.execute_sql.return_value = []

        all_data = pl.DataFrame({
            "idsector": [10, 20],
            "idproduto": [1, 2],
            "name": ["a", "b"],
        })

        with patch(
            "sql_databricks_bridge.core.diff_sync._extract_with_retry",
            return_value=([all_data], len(all_data)),
        ):
            stats = run_differential_sync(
                sql_client=mock_sql_client,
                dbx_client=mock_dbx_client,
                writer=mock_writer,
                sql_table="vw_artigoz",
                country="colombia",
                level1_column="idsector",
                level2_column="idproduto",
                fingerprint_table="bridge.events.sync_fingerprints",
                # No table_suffix
            )

        # Verify fingerprint queries reference 'vw_artigoz' (no suffix)
        select_calls = [
            c for c in mock_dbx_client.execute_sql.call_args_list
            if "SELECT" in str(c) and "table_name" in str(c)
        ]
        assert len(select_calls) >= 1
        assert "vw_artigoz'" in str(select_calls[0])
        # Make sure '_full' is NOT in the query
        assert "_full" not in str(select_calls[0])
