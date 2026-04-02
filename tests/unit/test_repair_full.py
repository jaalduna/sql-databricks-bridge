"""Unit tests for repair_full module — detecting and backfilling missing data in _full tables."""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from sql_databricks_bridge.core.delta_writer import WriteResult
from sql_databricks_bridge.core.repair_full import (
    find_missing_level1_values,
    parse_diff_sync_config,
    repair_country,
    repair_table,
)


@pytest.fixture
def mock_sql_client():
    """Mock SQL Server client."""
    client = MagicMock()
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
    writer.resolve_table_name.side_effect = lambda name, country, **kw: (
        f"`cat`.`{country}`.`{name}{kw.get('table_suffix', '') or ''}`"
    )
    writer.table_exists.return_value = True
    writer.append_dataframe.return_value = WriteResult(
        table_name="`cat`.`colombia`.`vw_artigoz_full`",
        rows=500,
        duration_seconds=2.0,
    )
    return writer


class TestFindMissingLevel1Values:
    def test_detects_missing_values(self, mock_dbx_client, mock_writer):
        """Should return values in base but not in _full table."""
        # Databricks LEFT ANTI JOIN returns missing sectors
        mock_dbx_client.execute_sql.return_value = [
            {"val": "30"},
            {"val": "40"},
        ]

        missing = find_missing_level1_values(
            mock_dbx_client, mock_writer,
            country="colombia", sql_table="vw_artigoz",
            level1_col="idsector", table_suffix="_full",
        )

        assert missing == ["30", "40"]
        # Verify query references both base and _full tables
        call_sql = str(mock_dbx_client.execute_sql.call_args)
        assert "vw_artigoz`" in call_sql
        assert "vw_artigoz_full`" in call_sql

    def test_no_missing_returns_empty(self, mock_dbx_client, mock_writer):
        """When all values exist in _full, return empty list."""
        mock_dbx_client.execute_sql.return_value = []

        missing = find_missing_level1_values(
            mock_dbx_client, mock_writer,
            country="colombia", sql_table="vw_artigoz",
            level1_col="idsector", table_suffix="_full",
        )

        assert missing == []

    def test_base_table_not_exists(self, mock_dbx_client, mock_writer):
        """When base table doesn't exist, return empty."""
        mock_writer.table_exists.side_effect = lambda name: "_full" in name

        missing = find_missing_level1_values(
            mock_dbx_client, mock_writer,
            country="colombia", sql_table="vw_artigoz",
            level1_col="idsector", table_suffix="_full",
        )

        assert missing == []


class TestRepairTable:
    def test_no_gaps_returns_ok(self, mock_sql_client, mock_dbx_client, mock_writer):
        """When no missing values, status should be 'ok' with no writes."""
        mock_dbx_client.execute_sql.return_value = []  # no missing values

        result = repair_table(
            sql_client=mock_sql_client,
            dbx_client=mock_dbx_client,
            writer=mock_writer,
            country="colombia",
            sql_table="vw_artigoz",
            level1_col="idsector",
            level2_col="idproduto",
            table_suffix="_full",
            query_sql="SELECT * FROM vw_artigoz",
            fingerprint_table="bridge.events.sync_fingerprints",
        )

        assert result.status == "ok"
        assert result.rows_inserted == 0
        mock_sql_client.execute_query.assert_not_called()
        mock_writer.append_dataframe.assert_not_called()

    def test_dry_run_reports_without_writing(
        self, mock_sql_client, mock_dbx_client, mock_writer
    ):
        """Dry run should detect gaps but not extract or write."""
        mock_dbx_client.execute_sql.return_value = [
            {"val": "30"},
            {"val": "40"},
        ]

        result = repair_table(
            sql_client=mock_sql_client,
            dbx_client=mock_dbx_client,
            writer=mock_writer,
            country="colombia",
            sql_table="vw_artigoz",
            level1_col="idsector",
            level2_col="idproduto",
            table_suffix="_full",
            query_sql="SELECT * FROM vw_artigoz",
            fingerprint_table="bridge.events.sync_fingerprints",
            dry_run=True,
        )

        assert result.status == "dry_run"
        assert result.missing_values == ["30", "40"]
        assert result.rows_inserted == 0
        mock_sql_client.execute_query.assert_not_called()
        mock_writer.append_dataframe.assert_not_called()

    def test_repair_extracts_and_appends_missing_slices(
        self, mock_sql_client, mock_dbx_client, mock_writer
    ):
        """Repair should extract missing slices from SQL and append to _full table."""
        # First call: find_missing returns 2 missing values
        # Second call: compute_level1_fingerprints returns fingerprints
        mock_dbx_client.execute_sql.side_effect = [
            [{"val": "30"}, {"val": "40"}],  # find_missing query
            None,  # DELETE old fingerprints
            None,  # INSERT new fingerprints
        ]

        repair_data = pl.DataFrame({
            "idsector": [30, 30, 40],
            "idproduto": [10, 11, 20],
            "name": ["x", "y", "z"],
        })
        # First execute_query call: extraction; second: fingerprints
        mock_sql_client.execute_query.side_effect = [
            repair_data,  # extraction
            pl.DataFrame({"grp_value": ["30", "40"], "cnt": [2, 1], "chk": [999, 888]}),  # fingerprints
        ]

        result = repair_table(
            sql_client=mock_sql_client,
            dbx_client=mock_dbx_client,
            writer=mock_writer,
            country="colombia",
            sql_table="vw_artigoz",
            level1_col="idsector",
            level2_col="idproduto",
            table_suffix="_full",
            query_sql="SELECT * FROM vw_artigoz",
            fingerprint_table="bridge.events.sync_fingerprints",
        )

        assert result.status == "repaired"
        assert result.rows_inserted == 3
        assert result.missing_values == ["30", "40"]
        # Verify append was called with the repair data
        mock_writer.append_dataframe.assert_called_once()
        call_args = mock_writer.append_dataframe.call_args
        assert call_args.kwargs.get("table_suffix") == "_full" or call_args[1].get("table_suffix") == "_full"

    def test_error_is_caught(self, mock_sql_client, mock_dbx_client, mock_writer):
        """Errors should be caught and result in error status."""
        mock_dbx_client.execute_sql.side_effect = Exception("connection lost")

        result = repair_table(
            sql_client=mock_sql_client,
            dbx_client=mock_dbx_client,
            writer=mock_writer,
            country="colombia",
            sql_table="vw_artigoz",
            level1_col="idsector",
            level2_col="idproduto",
            table_suffix="_full",
            query_sql="SELECT * FROM vw_artigoz",
            fingerprint_table="bridge.events.sync_fingerprints",
        )

        assert result.status == "error"
        assert "connection lost" in result.error


class TestParseDiffSyncConfig:
    def test_basic_parsing(self):
        cfg = parse_diff_sync_config(
            "vw_artigoz:idsector:idproduto,j_atoscompra_new:periodo:idproduto"
        )
        assert "vw_artigoz" in cfg
        assert cfg["vw_artigoz"]["level1_column"] == "idsector"
        assert cfg["vw_artigoz"]["level2_column"] == "idproduto"
        assert "j_atoscompra_new" in cfg
        assert cfg["j_atoscompra_new"]["level1_column"] == "periodo"

    def test_mutable_months(self):
        cfg = parse_diff_sync_config("hato_cabecalho:periodo:idproduto:2")
        assert cfg["hato_cabecalho"]["mutable_months"] == 2

    def test_checksum_columns(self):
        cfg = parse_diff_sync_config(
            "j_atoscompra_new:periodo:idproduto",
            "j_atoscompra_new:quantidade+preco+value_pm",
        )
        assert cfg["j_atoscompra_new"]["checksum_columns"] == ["quantidade", "preco", "value_pm"]

    def test_empty_string(self):
        cfg = parse_diff_sync_config("")
        assert cfg == {}


class TestRepairCountry:
    def test_repair_country_iterates_tables(
        self, mock_sql_client, mock_dbx_client, mock_writer
    ):
        """repair_country should iterate over all configured tables."""
        diff_cfg = {
            "vw_artigoz": {"level1_column": "idsector", "level2_column": "idproduto"},
            "mordom": {"level1_column": "periodo", "level2_column": "iddomicilio"},
        }

        # No missing values for any table
        mock_dbx_client.execute_sql.return_value = []

        with patch(
            "sql_databricks_bridge.core.repair_full.CountryAwareQueryLoader"
        ) as MockLoader:
            mock_loader = MockLoader.return_value
            mock_loader.get_query.return_value = "SELECT * FROM dummy"

            results = repair_country(
                sql_client=mock_sql_client,
                dbx_client=mock_dbx_client,
                writer=mock_writer,
                country="colombia",
                diff_tables_config=diff_cfg,
                queries_path="queries",
                fingerprint_table="bridge.events.sync_fingerprints",
                dry_run=True,
            )

        assert len(results) == 2
        assert all(r.status == "ok" for r in results)

    def test_repair_country_only_table_filter(
        self, mock_sql_client, mock_dbx_client, mock_writer
    ):
        """only_table should filter to a single table."""
        diff_cfg = {
            "vw_artigoz": {"level1_column": "idsector", "level2_column": "idproduto"},
            "mordom": {"level1_column": "periodo", "level2_column": "iddomicilio"},
        }

        mock_dbx_client.execute_sql.return_value = []

        with patch(
            "sql_databricks_bridge.core.repair_full.CountryAwareQueryLoader"
        ) as MockLoader:
            mock_loader = MockLoader.return_value
            mock_loader.get_query.return_value = "SELECT * FROM dummy"

            results = repair_country(
                sql_client=mock_sql_client,
                dbx_client=mock_dbx_client,
                writer=mock_writer,
                country="colombia",
                diff_tables_config=diff_cfg,
                queries_path="queries",
                fingerprint_table="bridge.events.sync_fingerprints",
                only_table="vw_artigoz",
            )

        assert len(results) == 1
        assert results[0].table_name == "vw_artigoz"
