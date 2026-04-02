"""Simulador Sync — read product attributes & volume equivalence from network share."""

from __future__ import annotations

import io
import json
import logging
import re
import zipfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import polars as pl

from sql_databricks_bridge.core.delta_writer import DeltaTableWriter

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Country code → schema mapping
# ---------------------------------------------------------------------------

DEFAULT_COUNTRY_MAP_STR = (
    "AR:argentina,BO:bolivia,CE:cam,CL:chile,CO:colombia,EC:ecuador,MX:mexico,PE:peru"
)


def parse_country_map(config_str: str) -> dict[str, str]:
    """Parse "AR:argentina,BO:bolivia,..." into {CC: country_name}."""
    mapping: dict[str, str] = {}
    for pair in config_str.split(","):
        pair = pair.strip()
        if ":" not in pair:
            continue
        cc, name = pair.split(":", 1)
        mapping[cc.strip().upper()] = name.strip().lower()
    return mapping


# ---------------------------------------------------------------------------
# Period discovery
# ---------------------------------------------------------------------------


@dataclass(frozen=True, order=True)
class SimuladorPeriod:
    """Represents a YYYY_MM folder period."""

    sort_key: int  # YYYYMM for natural ordering
    year: int
    month: int
    folder_name: str  # original folder name, e.g. "2026_02"


def _parse_period_folder(name: str) -> SimuladorPeriod | None:
    """Try to parse a folder name like '2026_02' into a SimuladorPeriod."""
    m = re.match(r"^(\d{4})_(\d{2})$", name)
    if not m:
        return None
    year, month = int(m.group(1)), int(m.group(2))
    if not (1 <= month <= 12):
        return None
    return SimuladorPeriod(
        sort_key=year * 100 + month,
        year=year,
        month=month,
        folder_name=name,
    )


def discover_periods(base_path: Path, data_type: str) -> list[SimuladorPeriod]:
    """List YYYY_MM period folders under base_path/{data_type}/, sorted ascending.

    data_type is "ATTRIBUTES" or "VOLEQ".
    """
    type_dir = base_path / data_type
    if not type_dir.is_dir():
        logger.warning("Directory does not exist: %s", type_dir)
        return []

    periods: list[SimuladorPeriod] = []
    for entry in type_dir.iterdir():
        if entry.is_dir():
            p = _parse_period_folder(entry.name)
            if p:
                periods.append(p)
    return sorted(periods)


def get_latest_period(base_path: Path) -> SimuladorPeriod | None:
    """Return the newest period present in BOTH ATTRIBUTES and VOLEQ."""
    attr_periods = set(p.folder_name for p in discover_periods(base_path, "ATTRIBUTES"))
    voleq_periods = set(p.folder_name for p in discover_periods(base_path, "VOLEQ"))
    common = attr_periods & voleq_periods
    if not common:
        return None

    parsed = [_parse_period_folder(f) for f in common]
    valid = [p for p in parsed if p is not None]
    return max(valid) if valid else None


# ---------------------------------------------------------------------------
# Zip file finders
# ---------------------------------------------------------------------------


def find_attributes_zip(base_path: Path, period: SimuladorPeriod, cc: str) -> Path | None:
    """Find {CC}_YYYY_MM.zip in ATTRIBUTES/YYYY_MM/."""
    folder = base_path / "ATTRIBUTES" / period.folder_name
    if not folder.is_dir():
        return None

    expected = f"{cc.upper()}_{period.folder_name}.zip"
    candidate = folder / expected
    if candidate.is_file():
        return candidate

    # Case-insensitive fallback
    for f in folder.iterdir():
        if f.name.upper() == expected.upper() and f.is_file():
            return f
    return None


def find_voleq_zip(base_path: Path, period: SimuladorPeriod, cc: str) -> Path | None:
    """Find VOLEQ zip for a country in VOLEQ/YYYY_MM/.

    Pattern: {CC}_{GUID}_{date}_{time}_S1004_import.zip[.zip]
    For MX: prefer files containing 'V2', fall back to regular.
    """
    folder = base_path / "VOLEQ" / period.folder_name
    if not folder.is_dir():
        return None

    cc_upper = cc.upper()
    pattern = re.compile(
        rf"^{re.escape(cc_upper)}_[0-9a-fA-F-]+_\d{{8}}_\d{{6}}_S1004_import(\.zip){{1,2}}$",
        re.IGNORECASE,
    )

    matches: list[Path] = []
    for f in folder.iterdir():
        if f.is_file() and pattern.match(f.name):
            matches.append(f)

    if not matches:
        return None

    # MX: prefer V2 variant
    if cc_upper == "MX":
        v2 = [m for m in matches if "V2" in m.name.upper()]
        if v2:
            matches = v2

    # Pick newest by modification time
    return max(matches, key=lambda p: p.stat().st_mtime)


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------


_ACCENT_MAP = str.maketrans(
    "áéíóúÁÉÍÓÚñÑüÜ",
    "aeiouAEIOUnNuU",
)


def _sanitize_name(name: str) -> str:
    """Sanitize a single column name for Delta Lake compatibility."""
    clean = name.translate(_ACCENT_MAP)
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", clean)
    clean = re.sub(r"_+", "_", clean).strip("_").lower()
    return clean or "col_unknown"


def _sanitize_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Sanitize column names for Delta Lake compatibility.

    Replaces spaces, brackets, parentheses, and special chars with underscores.
    Strips leading/trailing underscores and collapses consecutive underscores.
    """
    rename_map: dict[str, str] = {}
    for col in df.columns:
        rename_map[col] = _sanitize_name(col)
    return df.rename(rename_map)


# ---------------------------------------------------------------------------
# Column name mapping (PA ID → standardized name)
# ---------------------------------------------------------------------------

# Regex to extract [PA_ID] prefix from attribute column headers like "[59708] WP_Compañía"
_ATTR_COL_RE = re.compile(r"^\[(\d+)\]\s*(.+)$")


def load_attribute_mapping(base_path: Path, cc: str) -> dict[str, str]:
    """Load the attribute mapping CSV for a country code.

    Returns {PA_ID: internal_name} e.g. {"59708": "wp_compania"}.
    The mapping files live at ATTRIBUTES/{CC} - Product Classification Mapping para Simulador.csv
    """
    mapping_file = base_path / "ATTRIBUTES" / f"{cc.upper()} - Product Classification Mapping para Simulador.csv"
    if not mapping_file.is_file():
        logger.warning("No attribute mapping file for %s: %s", cc, mapping_file)
        return {}

    pa_map: dict[str, str] = {}
    try:
        raw = mapping_file.read_text(encoding="utf-8", errors="replace")
        for line in raw.strip().split("\n"):
            parts = line.split(",")
            if len(parts) < 4:
                continue
            element_type = parts[0].strip()
            if element_type not in ("Attribute", "Scope"):
                continue
            pa_id = parts[1].strip()
            display_name = parts[2].strip()  # e.g. "WP_Compañía"
            pa_map[pa_id] = _sanitize_name(display_name)
        logger.info("Loaded attribute mapping for %s: %d entries", cc, len(pa_map))
    except Exception as e:
        logger.warning("Failed to load attribute mapping for %s: %s", cc, e)
    return pa_map


def _rename_attribute_columns(df: pl.DataFrame, pa_map: dict[str, str]) -> pl.DataFrame:
    """Rename attribute columns from '[PA_ID] WP_Name' to the standardized internal name.

    Columns that don't match the [PA_ID] pattern are sanitized normally.
    """
    rename_map: dict[str, str] = {}
    for col in df.columns:
        m = _ATTR_COL_RE.match(col)
        if m:
            pa_id = m.group(1)
            if pa_id in pa_map:
                rename_map[col] = pa_map[pa_id]
            else:
                # PA ID not in mapping — use the display name part
                rename_map[col] = _sanitize_name(m.group(2))
        else:
            rename_map[col] = _sanitize_name(col)
    return df.rename(rename_map)


# VOLEQ column ID → human-readable name (from VolumeEquivalenceDefinition, same for all countries)
VOLEQ_ID_MAP: dict[str, str] = {
    "1": "units",
    "2": "grams",
    "3": "kilograms",
    "4": "mililitres",
    "5": "litres",
    "6": "pg_stat_units",
    "17": "nb_of_cups",
    "20": "litres_yield",
    "23": "nb_of_doses",
    "28": "nb_of_packages",
    "57": "kg_l",
    "71": "kg_l_units_fmcg",
    "98": "sheets",
    "108": "blades",
    "109": "portions",
    "110": "int_units",
    "111": "meters",
    "115": "gr_ml",
    "120": "pounds",
    "121": "ounces",
    "122": "fluid_ounces",
}


def _rename_voleq_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Rename VOLEQ numeric column IDs to human-readable names."""
    rename_map: dict[str, str] = {}
    for col in df.columns:
        if col.strip().isdigit() and col.strip() in VOLEQ_ID_MAP:
            rename_map[col] = VOLEQ_ID_MAP[col.strip()]
        else:
            rename_map[col] = _sanitize_name(col)
    return df.rename(rename_map)


def _detect_separator(header_line: str) -> str:
    """Auto-detect ';' vs ',' separator by counting occurrences in header."""
    semicolons = header_line.count(";")
    commas = header_line.count(",")
    return ";" if semicolons >= commas else ","


def _find_csv_in_zip(zf: zipfile.ZipFile, suffix: str = ".csv") -> str | None:
    """Find the first CSV file in a ZipFile, preferring *Data.csv."""
    names = [
        n for n in zf.namelist()
        if n.lower().endswith(suffix) and not n.startswith("__MACOSX")
    ]
    if not names:
        return None
    # Prefer *Data.csv
    data_csvs = [n for n in names if n.lower().endswith("data.csv")]
    return data_csvs[0] if data_csvs else names[0]


def parse_attributes_csv(
    zip_path: Path, pa_map: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Open attributes zip, find *Data.csv, parse semicolon CSV → Polars DataFrame.

    If *pa_map* is provided, attribute columns are renamed to standardized names.
    Otherwise falls back to generic sanitization.
    """
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_name = _find_csv_in_zip(zf)
        if csv_name is None:
            raise ValueError(f"No CSV file found in {zip_path}")

        raw = zf.read(csv_name)

    # Detect separator from first line
    first_line = raw.split(b"\n", 1)[0].decode("utf-8", errors="replace")
    sep = _detect_separator(first_line)

    df = pl.read_csv(
        io.BytesIO(raw),
        separator=sep,
        infer_schema_length=10000,
        ignore_errors=True,
        truncate_ragged_lines=True,
    )
    if pa_map:
        df = _rename_attribute_columns(df, pa_map)
    else:
        df = _sanitize_columns(df)
    logger.info(
        "Parsed attributes CSV: %d rows, %d cols from %s",
        len(df), len(df.columns), zip_path.name,
    )
    return df


def _extract_voleq_content(zip_path: Path) -> bytes:
    """Extract CSV content from a VOLEQ zip, handling double-zip and nested dirs.

    VOLEQ zips may be:
    1. A normal zip containing a CSV directly
    2. A .zip.zip (outer zip contains an inner zip which has the CSV)
    3. A zip with nested directories containing the CSV
    """
    with zipfile.ZipFile(zip_path, "r") as outer:
        # Look for CSV directly
        csv_name = _find_csv_in_zip(outer)
        if csv_name is not None:
            return outer.read(csv_name)

        # Look for inner zip (double-zip case)
        inner_zips = [n for n in outer.namelist() if n.lower().endswith(".zip")]
        if inner_zips:
            inner_data = outer.read(inner_zips[0])
            with zipfile.ZipFile(io.BytesIO(inner_data), "r") as inner:
                csv_name = _find_csv_in_zip(inner)
                if csv_name is not None:
                    return inner.read(csv_name)

    raise ValueError(
        f"No CSV found in VOLEQ zip (tried direct + inner zip): {zip_path}"
    )


def parse_voleq_csv(zip_path: Path) -> pl.DataFrame:
    """Parse VOLEQ CSV from zip, handling double-zip + auto-detecting separator."""
    raw = _extract_voleq_content(zip_path)

    # Detect separator
    first_line = raw.split(b"\n", 1)[0].decode("utf-8", errors="replace")
    sep = _detect_separator(first_line)

    df = pl.read_csv(
        io.BytesIO(raw),
        separator=sep,
        infer_schema_length=10000,
        ignore_errors=True,
        truncate_ragged_lines=True,
    )
    df = _rename_voleq_columns(df)
    logger.info(
        "Parsed VOLEQ CSV: %d rows, %d cols from %s",
        len(df), len(df.columns), zip_path.name,
    )
    return df


# ---------------------------------------------------------------------------
# Sync result types
# ---------------------------------------------------------------------------


@dataclass
class CountrySyncResult:
    """Result for one country + data_type."""

    country_code: str
    country_name: str
    data_type: str  # "attributes" or "voleq"
    status: str = "pending"
    rows: int = 0
    table_name: str = ""
    duration_s: float = 0.0
    error: str = ""


@dataclass
class SyncRoundResult:
    """Result of a full sync round."""

    period: str
    started_at: str = ""
    finished_at: str = ""
    results: list[CountrySyncResult] = field(default_factory=list)
    dry_run: bool = False


# ---------------------------------------------------------------------------
# SimuladorSyncer
# ---------------------------------------------------------------------------

DATA_TYPES = ("attributes", "voleq")


class SimuladorSyncer:
    """Reads SIMULADOR data from network share and uploads to Databricks Delta tables."""

    def __init__(
        self,
        writer: DeltaTableWriter,
        base_path: str | Path,
        country_map: dict[str, str] | None = None,
    ) -> None:
        self.writer = writer
        self.base_path = Path(base_path)
        self.country_map = country_map or parse_country_map(DEFAULT_COUNTRY_MAP_STR)

    def sync_country(
        self,
        cc: str,
        period: SimuladorPeriod,
        data_types: list[str] | None = None,
        dry_run: bool = False,
    ) -> list[CountrySyncResult]:
        """Sync one country for the given period.

        Args:
            cc: 2-letter country code (e.g. "BO").
            period: Period to sync.
            data_types: Subset of ["attributes", "voleq"]. None = both.
            dry_run: If True, parse only — don't upload to Databricks.

        Returns:
            List of CountrySyncResult (one per data_type).
        """
        cc_upper = cc.upper()
        country_name = self.country_map.get(cc_upper)
        if not country_name:
            return [CountrySyncResult(
                country_code=cc_upper,
                country_name="unknown",
                data_type="all",
                status="error",
                error=f"Unknown country code: {cc_upper}",
            )]

        types_to_sync = data_types or list(DATA_TYPES)
        results: list[CountrySyncResult] = []

        for dtype in types_to_sync:
            result = self._sync_one(cc_upper, country_name, period, dtype, dry_run)
            results.append(result)

        return results

    def sync_all(
        self,
        period: SimuladorPeriod | None = None,
        countries: list[str] | None = None,
        data_types: list[str] | None = None,
        dry_run: bool = False,
    ) -> SyncRoundResult:
        """Sync all (or specified) countries for a period.

        Args:
            period: Period to sync. None = auto-discover latest.
            countries: Country codes to sync. None = all from country_map.
            data_types: Subset of ["attributes", "voleq"]. None = both.
            dry_run: If True, parse only — don't upload.

        Returns:
            SyncRoundResult with all per-country results.
        """
        started = datetime.now()

        # Resolve period
        if period is None:
            period = get_latest_period(self.base_path)
            if period is None:
                return SyncRoundResult(
                    period="none",
                    started_at=started.isoformat(timespec="seconds"),
                    finished_at=datetime.now().isoformat(timespec="seconds"),
                    results=[CountrySyncResult(
                        country_code="ALL",
                        country_name="all",
                        data_type="all",
                        status="error",
                        error="No common period found in ATTRIBUTES and VOLEQ folders",
                    )],
                    dry_run=dry_run,
                )
            logger.info("Auto-discovered latest period: %s", period.folder_name)

        # Resolve countries
        cc_list = countries or list(self.country_map.keys())

        round_result = SyncRoundResult(
            period=period.folder_name,
            started_at=started.isoformat(timespec="seconds"),
            dry_run=dry_run,
        )

        for cc in cc_list:
            country_results = self.sync_country(cc.upper(), period, data_types, dry_run)
            round_result.results.extend(country_results)

        round_result.finished_at = datetime.now().isoformat(timespec="seconds")
        return round_result

    # ------------------------------------------------------------------
    # Change detection for periodic sync
    # ------------------------------------------------------------------

    @staticmethod
    def _state_file() -> Path:
        """Path to the JSON file that stores last-synced file mtimes."""
        p = Path(".bridge_data") / "simulador_state.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    @staticmethod
    def _load_state(state_file: Path) -> dict[str, dict]:
        """Load {CC:dtype -> {mtime, period, path}} from JSON."""
        if not state_file.is_file():
            return {}
        try:
            return json.loads(state_file.read_text(encoding="utf-8"))
        except Exception:
            return {}

    @staticmethod
    def _save_state(state_file: Path, state: dict[str, dict]) -> None:
        state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def detect_changes(self) -> dict[str, list[str]]:
        """Compare network-share file mtimes with stored state.

        Returns {CC: [data_types_changed]} for countries that need re-sync.
        Only triggers if a file is newer or a new period appeared.
        """
        state_file = self._state_file()
        prev = self._load_state(state_file)
        changes: dict[str, list[str]] = {}

        latest = get_latest_period(self.base_path)
        if latest is None:
            return changes

        for cc in self.country_map:
            for dtype in DATA_TYPES:
                zip_path, actual_period = self._find_zip_with_fallback(cc, latest, dtype)
                if zip_path is None:
                    continue

                key = f"{cc}:{dtype}"
                try:
                    mtime = zip_path.stat().st_mtime
                except OSError:
                    continue

                prev_entry = prev.get(key, {})
                prev_mtime = prev_entry.get("mtime", 0.0)
                prev_period = prev_entry.get("period", "")

                if mtime > prev_mtime or actual_period.folder_name != prev_period:
                    changes.setdefault(cc, []).append(dtype)

        return changes

    def save_sync_state(self, results: list[CountrySyncResult]) -> None:
        """Update stored state for successfully synced items."""
        state_file = self._state_file()
        state = self._load_state(state_file)

        latest = get_latest_period(self.base_path)
        if latest is None:
            return

        for r in results:
            if r.status != "completed":
                continue
            cc = r.country_code
            dtype = r.data_type
            zip_path, actual_period = self._find_zip_with_fallback(cc, latest, dtype)
            if zip_path is None:
                continue
            try:
                mtime = zip_path.stat().st_mtime
            except OSError:
                continue

            key = f"{cc}:{dtype}"
            state[key] = {
                "mtime": mtime,
                "period": actual_period.folder_name,
                "path": str(zip_path),
                "synced_at": datetime.now().isoformat(timespec="seconds"),
            }

        self._save_state(state_file, state)

    def sync_changes_only(self) -> SyncRoundResult | None:
        """Detect changes on network share and sync only what changed.

        Returns None if no changes detected, or a SyncRoundResult with results.
        """
        changes = self.detect_changes()
        if not changes:
            logger.info("Simulador: no changes detected on network share")
            return None

        logger.info("Simulador: changes detected for %s", changes)
        started = datetime.now()
        latest = get_latest_period(self.base_path)
        if latest is None:
            return None

        round_result = SyncRoundResult(
            period=latest.folder_name,
            started_at=started.isoformat(timespec="seconds"),
        )

        for cc, dtypes in changes.items():
            for dtype in dtypes:
                cc_upper = cc.upper()
                country_name = self.country_map.get(cc_upper, "unknown")
                result = self._sync_one(cc_upper, country_name, latest, dtype, dry_run=False)
                round_result.results.append(result)

        round_result.finished_at = datetime.now().isoformat(timespec="seconds")

        # Save state for completed items
        self.save_sync_state(round_result.results)
        return round_result

    def _find_zip_with_fallback(
        self,
        cc: str,
        period: SimuladorPeriod,
        data_type: str,
    ) -> tuple[Path | None, SimuladorPeriod]:
        """Find zip for a country, falling back to previous periods if not found.

        Returns (zip_path, actual_period) — zip_path is None if not found in any period.
        """
        finder = find_attributes_zip if data_type == "attributes" else find_voleq_zip

        # Try requested period first
        zip_path = finder(self.base_path, period, cc)
        if zip_path is not None:
            return zip_path, period

        # Fallback: try older periods in descending order
        folder_type = "ATTRIBUTES" if data_type == "attributes" else "VOLEQ"
        all_periods = discover_periods(self.base_path, folder_type)
        # Only try periods older than the requested one, newest first
        fallback_periods = sorted(
            [p for p in all_periods if p.sort_key < period.sort_key],
            reverse=True,
        )

        for fb_period in fallback_periods:
            zip_path = finder(self.base_path, fb_period, cc)
            if zip_path is not None:
                logger.info(
                    "Fallback: %s/%s not in %s, found in %s",
                    cc, data_type, period.folder_name, fb_period.folder_name,
                )
                return zip_path, fb_period

        return None, period

    def _sync_one(
        self,
        cc: str,
        country_name: str,
        period: SimuladorPeriod,
        data_type: str,
        dry_run: bool,
    ) -> CountrySyncResult:
        """Sync a single (country, data_type) pair."""
        result = CountrySyncResult(
            country_code=cc,
            country_name=country_name,
            data_type=data_type,
        )
        start = datetime.now()

        try:
            # Find zip (with period fallback)
            if data_type == "attributes":
                zip_path, actual_period = self._find_zip_with_fallback(cc, period, "attributes")
                table_name = "product_attributes"
            elif data_type == "voleq":
                zip_path, actual_period = self._find_zip_with_fallback(cc, period, "voleq")
                table_name = "volume_equivalence"
            else:
                result.status = "error"
                result.error = f"Unknown data_type: {data_type}"
                return result

            if zip_path is None:
                result.status = "skipped"
                result.error = f"No zip found for {cc} {data_type} in any period"
                logger.warning(result.error)
                return result

            logger.info(
                "Found %s zip for %s: %s (period=%s)",
                data_type, cc, zip_path.name, actual_period.folder_name,
            )

            # Parse CSV
            if data_type == "attributes":
                pa_map = load_attribute_mapping(self.base_path, cc)
                df = parse_attributes_csv(zip_path, pa_map=pa_map)
            else:
                df = parse_voleq_csv(zip_path)

            result.rows = len(df)

            if dry_run:
                result.status = "dry_run"
                result.table_name = f"(dry_run) {country_name}.{table_name}"
                logger.info(
                    "[DRY RUN] %s/%s: %d rows, %d cols",
                    cc, data_type, len(df), len(df.columns),
                )
            else:
                # Upload to Databricks
                write_result = self.writer.write_dataframe(
                    df,
                    query_name=table_name,
                    country=country_name,
                )
                result.table_name = write_result.table_name
                result.status = "completed"
                logger.info(
                    "Uploaded %s/%s: %d rows -> %s (%.1fs)",
                    cc, data_type, write_result.rows,
                    write_result.table_name, write_result.duration_seconds,
                )

        except Exception as e:
            result.status = "error"
            result.error = str(e)
            logger.error("Failed %s/%s: %s", cc, data_type, e, exc_info=True)

        result.duration_s = (datetime.now() - start).total_seconds()
        return result
