"""Unit tests for simulador_sync module."""

import io
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from sql_databricks_bridge.core.delta_writer import DeltaTableWriter, WriteResult
from sql_databricks_bridge.core.simulador_sync import (
    SimuladorPeriod,
    SimuladorSyncer,
    _detect_separator,
    _parse_period_folder,
    discover_periods,
    find_attributes_zip,
    find_voleq_zip,
    get_latest_period,
    parse_attributes_csv,
    parse_country_map,
    parse_voleq_csv,
)


# ---------------------------------------------------------------------------
# parse_country_map
# ---------------------------------------------------------------------------


class TestParseCountryMap:
    def test_basic(self):
        result = parse_country_map("AR:argentina,BO:bolivia")
        assert result == {"AR": "argentina", "BO": "bolivia"}

    def test_full_default(self):
        result = parse_country_map(
            "AR:argentina,BO:bolivia,CE:cam,CL:chile,CO:colombia,EC:ecuador,MX:mexico,PE:peru"
        )
        assert len(result) == 8
        assert result["CE"] == "cam"
        assert result["MX"] == "mexico"

    def test_whitespace(self):
        result = parse_country_map(" AR : argentina , BO : bolivia ")
        assert result == {"AR": "argentina", "BO": "bolivia"}

    def test_empty_string(self):
        assert parse_country_map("") == {}

    def test_malformed_entries_skipped(self):
        result = parse_country_map("AR:argentina,BADENTRY,BO:bolivia")
        assert result == {"AR": "argentina", "BO": "bolivia"}


# ---------------------------------------------------------------------------
# Period parsing & discovery
# ---------------------------------------------------------------------------


class TestParsePeriodFolder:
    def test_valid(self):
        p = _parse_period_folder("2026_02")
        assert p is not None
        assert p.year == 2026
        assert p.month == 2
        assert p.folder_name == "2026_02"
        assert p.sort_key == 202602

    def test_invalid_format(self):
        assert _parse_period_folder("202602") is None
        assert _parse_period_folder("2026-02") is None
        assert _parse_period_folder("abc") is None

    def test_invalid_month(self):
        assert _parse_period_folder("2026_13") is None
        assert _parse_period_folder("2026_00") is None

    def test_ordering(self):
        p1 = _parse_period_folder("2025_12")
        p2 = _parse_period_folder("2026_01")
        assert p1 < p2


class TestDiscoverPeriods:
    def test_discovers_valid_folders(self, tmp_path):
        attr_dir = tmp_path / "ATTRIBUTES"
        attr_dir.mkdir()
        (attr_dir / "2026_01").mkdir()
        (attr_dir / "2026_02").mkdir()
        (attr_dir / "not_a_period").mkdir()
        (attr_dir / "readme.txt").write_text("hi")

        periods = discover_periods(tmp_path, "ATTRIBUTES")
        assert len(periods) == 2
        assert periods[0].folder_name == "2026_01"
        assert periods[1].folder_name == "2026_02"

    def test_missing_directory(self, tmp_path):
        periods = discover_periods(tmp_path, "ATTRIBUTES")
        assert periods == []


class TestGetLatestPeriod:
    def test_common_period(self, tmp_path):
        (tmp_path / "ATTRIBUTES" / "2026_01").mkdir(parents=True)
        (tmp_path / "ATTRIBUTES" / "2026_02").mkdir(parents=True)
        (tmp_path / "VOLEQ" / "2026_01").mkdir(parents=True)
        (tmp_path / "VOLEQ" / "2026_03").mkdir(parents=True)

        latest = get_latest_period(tmp_path)
        assert latest is not None
        assert latest.folder_name == "2026_01"

    def test_no_common(self, tmp_path):
        (tmp_path / "ATTRIBUTES" / "2026_01").mkdir(parents=True)
        (tmp_path / "VOLEQ" / "2026_02").mkdir(parents=True)

        assert get_latest_period(tmp_path) is None

    def test_multiple_common(self, tmp_path):
        for p in ["2025_11", "2025_12", "2026_01"]:
            (tmp_path / "ATTRIBUTES" / p).mkdir(parents=True)
            (tmp_path / "VOLEQ" / p).mkdir(parents=True)

        latest = get_latest_period(tmp_path)
        assert latest.folder_name == "2026_01"


# ---------------------------------------------------------------------------
# Zip finders
# ---------------------------------------------------------------------------


class TestFindAttributesZip:
    def test_finds_exact(self, tmp_path):
        period = SimuladorPeriod(sort_key=202602, year=2026, month=2, folder_name="2026_02")
        folder = tmp_path / "ATTRIBUTES" / "2026_02"
        folder.mkdir(parents=True)
        (folder / "BO_2026_02.zip").write_bytes(b"fake")

        result = find_attributes_zip(tmp_path, period, "BO")
        assert result is not None
        assert result.name == "BO_2026_02.zip"

    def test_case_insensitive(self, tmp_path):
        period = SimuladorPeriod(sort_key=202602, year=2026, month=2, folder_name="2026_02")
        folder = tmp_path / "ATTRIBUTES" / "2026_02"
        folder.mkdir(parents=True)
        (folder / "bo_2026_02.zip").write_bytes(b"fake")

        result = find_attributes_zip(tmp_path, period, "BO")
        assert result is not None

    def test_not_found(self, tmp_path):
        period = SimuladorPeriod(sort_key=202602, year=2026, month=2, folder_name="2026_02")
        folder = tmp_path / "ATTRIBUTES" / "2026_02"
        folder.mkdir(parents=True)

        assert find_attributes_zip(tmp_path, period, "BO") is None

    def test_missing_folder(self, tmp_path):
        period = SimuladorPeriod(sort_key=202602, year=2026, month=2, folder_name="2026_02")
        assert find_attributes_zip(tmp_path, period, "BO") is None


class TestFindVoleqZip:
    def _make_voleq_zip(self, folder: Path, name: str) -> Path:
        path = folder / name
        path.write_bytes(b"fake")
        return path

    def test_finds_matching_zip(self, tmp_path):
        period = SimuladorPeriod(sort_key=202602, year=2026, month=2, folder_name="2026_02")
        folder = tmp_path / "VOLEQ" / "2026_02"
        folder.mkdir(parents=True)
        self._make_voleq_zip(folder, "BO_abc12345-def6-7890-abcd-ef1234567890_20260215_120000_S1004_import.zip")

        result = find_voleq_zip(tmp_path, period, "BO")
        assert result is not None
        assert "BO_" in result.name

    def test_double_zip_extension(self, tmp_path):
        period = SimuladorPeriod(sort_key=202602, year=2026, month=2, folder_name="2026_02")
        folder = tmp_path / "VOLEQ" / "2026_02"
        folder.mkdir(parents=True)
        self._make_voleq_zip(folder, "CO_abc12345-def6-7890-abcd-ef1234567890_20260215_120000_S1004_import.zip.zip")

        result = find_voleq_zip(tmp_path, period, "CO")
        assert result is not None

    def test_not_found(self, tmp_path):
        period = SimuladorPeriod(sort_key=202602, year=2026, month=2, folder_name="2026_02")
        folder = tmp_path / "VOLEQ" / "2026_02"
        folder.mkdir(parents=True)

        assert find_voleq_zip(tmp_path, period, "BO") is None


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------


def _make_zip_with_csv(csv_content: str, csv_name: str = "TestData.csv") -> Path:
    """Create an in-memory zip containing a CSV and return as bytes."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(csv_name, csv_content)
    return buf.getvalue()


def _write_zip_file(tmp_path: Path, name: str, csv_content: str, csv_name: str = "TestData.csv") -> Path:
    """Write a zip file to disk."""
    path = tmp_path / name
    path.write_bytes(_make_zip_with_csv(csv_content, csv_name))
    return path


class TestDetectSeparator:
    def test_semicolon(self):
        assert _detect_separator("col1;col2;col3") == ";"

    def test_comma(self):
        assert _detect_separator("col1,col2,col3") == ","

    def test_equal_prefers_semicolon(self):
        assert _detect_separator("a;b,c") == ";"

    def test_more_semicolons(self):
        assert _detect_separator("a;b;c;d,e") == ";"


class TestParseAttributesCsv:
    def test_semicolon_csv(self, tmp_path):
        csv = "PA_ID;WP_Name;Value\n1;Prod A;100\n2;Prod B;200\n"
        path = _write_zip_file(tmp_path, "BO_2026_02.zip", csv, "BO_2026_02Data.csv")

        df = parse_attributes_csv(path)
        assert len(df) == 2
        assert "pa_id" in df.columns
        assert "wp_name" in df.columns

    def test_comma_csv(self, tmp_path):
        csv = "PA_ID,WP_Name,Value\n1,Prod A,100\n2,Prod B,200\n"
        path = _write_zip_file(tmp_path, "AR_2026_02.zip", csv, "AR_2026_02Data.csv")

        df = parse_attributes_csv(path)
        assert len(df) == 2

    def test_no_csv_raises(self, tmp_path):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "no csv here")
        path = tmp_path / "empty.zip"
        path.write_bytes(buf.getvalue())

        with pytest.raises(ValueError, match="No CSV"):
            parse_attributes_csv(path)


class TestParseVoleqCsv:
    def test_direct_csv(self, tmp_path):
        csv = "ID;Vol;Eq\n1;10;20\n2;30;40\n"
        path = _write_zip_file(tmp_path, "voleq.zip", csv, "voleq_data.csv")

        df = parse_voleq_csv(path)
        assert len(df) == 2

    def test_double_zip(self, tmp_path):
        """Test zip-within-zip (double zip)."""
        csv = "ID;Vol;Eq\n1;10;20\n"

        # Inner zip
        inner_buf = io.BytesIO()
        with zipfile.ZipFile(inner_buf, "w") as inner_zf:
            inner_zf.writestr("data.csv", csv)

        # Outer zip containing inner zip
        outer_buf = io.BytesIO()
        with zipfile.ZipFile(outer_buf, "w") as outer_zf:
            outer_zf.writestr("inner.zip", inner_buf.getvalue())

        path = tmp_path / "double.zip"
        path.write_bytes(outer_buf.getvalue())

        df = parse_voleq_csv(path)
        assert len(df) == 1
        assert "id" in df.columns

    def test_no_csv_raises(self, tmp_path):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "no csv")
        path = tmp_path / "nocsvs.zip"
        path.write_bytes(buf.getvalue())

        with pytest.raises(ValueError, match="No CSV found"):
            parse_voleq_csv(path)


# ---------------------------------------------------------------------------
# SimuladorSyncer
# ---------------------------------------------------------------------------


class TestSimuladorSyncer:
    @pytest.fixture
    def mock_writer(self):
        writer = MagicMock(spec=DeltaTableWriter)
        writer.write_dataframe.return_value = WriteResult(
            table_name="`catalog`.`country`.`table`",
            rows=100,
            duration_seconds=1.5,
        )
        return writer

    @pytest.fixture
    def syncer_setup(self, tmp_path, mock_writer):
        """Set up a syncer with a fake file structure."""
        # Create period folders
        attr_dir = tmp_path / "ATTRIBUTES" / "2026_02"
        attr_dir.mkdir(parents=True)
        voleq_dir = tmp_path / "VOLEQ" / "2026_02"
        voleq_dir.mkdir(parents=True)

        # Create attributes zip for BO
        csv = "PA_ID;WP_Name;Value\n1;Prod;100\n"
        _write_zip_file(attr_dir, "BO_2026_02.zip", csv, "BO_2026_02Data.csv")

        # Create voleq zip for BO
        csv = "ID;Vol;Eq\n1;10;20\n"
        voleq_name = "BO_abc12345-def6-7890-abcd-ef1234567890_20260215_120000_S1004_import.zip"
        _write_zip_file(voleq_dir, voleq_name, csv, "data.csv")

        country_map = {"BO": "bolivia"}
        syncer = SimuladorSyncer(writer=mock_writer, base_path=tmp_path, country_map=country_map)
        period = _parse_period_folder("2026_02")
        return syncer, period

    def test_sync_country_dry_run(self, syncer_setup):
        syncer, period = syncer_setup
        results = syncer.sync_country("BO", period, dry_run=True)
        assert len(results) == 2
        for r in results:
            assert r.status in ("dry_run", "skipped")
            assert r.country_code == "BO"

    def test_sync_country_upload(self, syncer_setup, mock_writer):
        syncer, period = syncer_setup
        results = syncer.sync_country("BO", period, dry_run=False)
        # At least one should be completed (attributes should work)
        completed = [r for r in results if r.status == "completed"]
        assert len(completed) >= 1
        mock_writer.write_dataframe.assert_called()

    def test_sync_country_unknown_code(self, syncer_setup):
        syncer, period = syncer_setup
        results = syncer.sync_country("XX", period)
        assert len(results) == 1
        assert results[0].status == "error"
        assert "Unknown country code" in results[0].error

    def test_sync_country_specific_type(self, syncer_setup):
        syncer, period = syncer_setup
        results = syncer.sync_country("BO", period, data_types=["attributes"], dry_run=True)
        assert len(results) == 1
        assert results[0].data_type == "attributes"

    def test_sync_all_auto_period(self, syncer_setup):
        syncer, period = syncer_setup
        round_result = syncer.sync_all(dry_run=True)
        assert round_result.period == "2026_02"
        assert len(round_result.results) >= 1

    def test_sync_all_explicit_period(self, syncer_setup):
        syncer, period = syncer_setup
        round_result = syncer.sync_all(period=period, dry_run=True)
        assert round_result.period == "2026_02"

    def test_sync_all_no_period_found(self, tmp_path, mock_writer):
        syncer = SimuladorSyncer(writer=mock_writer, base_path=tmp_path, country_map={"BO": "bolivia"})
        round_result = syncer.sync_all(dry_run=True)
        assert round_result.period == "none"
        assert round_result.results[0].status == "error"

    def test_sync_country_missing_zip_skipped(self, tmp_path, mock_writer):
        """Country with no zip file should be skipped, not error."""
        attr_dir = tmp_path / "ATTRIBUTES" / "2026_02"
        attr_dir.mkdir(parents=True)
        voleq_dir = tmp_path / "VOLEQ" / "2026_02"
        voleq_dir.mkdir(parents=True)

        syncer = SimuladorSyncer(
            writer=mock_writer, base_path=tmp_path, country_map={"AR": "argentina"}
        )
        period = _parse_period_folder("2026_02")
        results = syncer.sync_country("AR", period, dry_run=True)
        assert all(r.status == "skipped" for r in results)
