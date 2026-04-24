"""Metadata API endpoints -- country, query, and stage discovery."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.core.country_query_loader import CountryAwareQueryLoader
from sql_databricks_bridge.core.stages import load_stages
from sql_databricks_bridge.db import local_store
from sql_databricks_bridge.db import eligibility_store
from sql_databricks_bridge.db.sql_server import SQLServerClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/metadata", tags=["Metadata"])

# Module-level cached loader — avoids re-discovering queries on every request.
_loader: CountryAwareQueryLoader | None = None


def _get_loader() -> CountryAwareQueryLoader:
    global _loader
    if _loader is None:
        _loader = CountryAwareQueryLoader(Path(get_settings().queries_path))
    return _loader


class CountryInfo(BaseModel):
    code: str
    queries: list[str]
    queries_count: int
    type: str = "country"  # "country" or "server"


class CountriesResponse(BaseModel):
    countries: list[CountryInfo]


class StageInfo(BaseModel):
    code: str
    name: str


class StagesResponse(BaseModel):
    stages: list[StageInfo]


@router.get(
    "/countries",
    response_model=CountriesResponse,
    summary="List available countries and queries",
    description="Returns the list of supported countries and their available SQL queries.",
)
async def list_countries() -> CountriesResponse:
    """List all available countries, servers, and their queries."""
    import asyncio

    loader = _get_loader()
    settings = get_settings()
    disabled = {
        e.strip() for e in (settings.disabled_entries or "").split(",") if e.strip()
    }
    disabled_queries: dict[str, set[str]] = {}
    for pair in (settings.disabled_queries or "").split(","):
        pair = pair.strip()
        if ":" not in pair:
            continue
        entry, query = pair.split(":", 1)
        disabled_queries.setdefault(entry.strip(), set()).add(query.strip())

    entries = [(n, t) for n, t in loader.list_all_entries() if n not in disabled]

    # Discover queries in parallel — each hit to the network share can take
    # ~10 s to timeout, so sequential discovery for 14 entries is too slow.
    def _discover(name: str) -> list[str]:
        return loader.list_queries(name)

    loop = asyncio.get_running_loop()
    tasks = [loop.run_in_executor(None, _discover, name) for name, _ in entries]
    query_lists = await asyncio.gather(*tasks)

    result = []
    for (name, entry_type), queries in zip(entries, query_lists):
        hidden = disabled_queries.get(name, set())
        filtered = [q for q in queries if q not in hidden]
        result.append(
            CountryInfo(
                code=name,
                queries=filtered,
                queries_count=len(filtered),
                type=entry_type,
            )
        )

    return CountriesResponse(countries=result)


@router.get(
    "/stages",
    response_model=StagesResponse,
    summary="List available stages",
    description="Returns the list of pipeline stages.",
)
async def list_stages() -> StagesResponse:
    """List all available stages from YAML config."""
    rows = load_stages(get_settings().stages_file)
    return StagesResponse(
        stages=[StageInfo(code=r["code"], name=r["name"]) for r in rows]
    )


# -- Data availability (SQL Server) -----------------------------------------


class CountryAvailability(BaseModel):
    elegibilidad: bool = False
    pesaje: bool = False


class DataAvailabilityResponse(BaseModel):
    period: str
    countries: dict[str, CountryAvailability]


def _check_country_availability(country: str, year: int, month: int) -> tuple[str, CountryAvailability]:
    """Check pesaje & elegibilidad tables for *country* on SQL Server.

    Runs synchronously (blocking I/O) – intended to be called inside a
    ThreadPoolExecutor so multiple countries are checked in parallel.
    """
    try:
        client = SQLServerClient(country=country)

        # Pesaje check – if pesaje exists, elegibilidad is implied
        pesaje_df = client.execute_query(
            f"SELECT TOP 1 1 AS flag FROM rg_domicilios_pesos WHERE ano = {year} AND messem = '{month}'"
        )
        has_pesaje = len(pesaje_df) > 0

        if has_pesaje:
            return country, CountryAvailability(elegibilidad=True, pesaje=True)

        # Elegibilidad check (only when pesaje is absent)
        eleg_df = client.execute_query(
            f"SELECT TOP 1 1 AS flag FROM mordom WHERE ano = {year} AND mes = '{month}'"
        )
        has_eleg = len(eleg_df) > 0

        return country, CountryAvailability(elegibilidad=has_eleg, pesaje=False)

    except Exception:
        logger.warning("SQL Server unavailable for country=%s", country, exc_info=True)
        return country, CountryAvailability()


@router.get(
    "/data-availability",
    response_model=DataAvailabilityResponse,
    summary="Check data availability per country",
    description="Queries on-premise SQL Server to check whether elegibilidad and pesaje data exist for the given period.",
)
async def data_availability(
    period: str = Query(..., pattern=r"^\d{6}$", description="Period in YYYYMM format"),
) -> DataAvailabilityResponse:
    """Return pesaje / elegibilidad availability for every known country."""
    queries_base = Path(get_settings().queries_path)
    countries_path = queries_base / "countries"

    country_codes: list[str] = []
    if countries_path.exists():
        for d in sorted(countries_path.iterdir()):
            if d.is_dir() and not d.name.startswith("."):
                country_codes.append(d.name)

    # Mock mode: all countries available (for testing without SQL Server)
    if get_settings().mock_data_availability:
        logger.info("MOCK_DATA_AVAILABILITY: returning all countries as available")
        results = {code: CountryAvailability(elegibilidad=True, pesaje=True) for code in country_codes}
        return DataAvailabilityResponse(period=period, countries=results)

    year = int(period[:4])
    month = int(period[4:])
    results: dict[str, CountryAvailability] = {}

    if not country_codes:
        return DataAvailabilityResponse(period=period, countries=results)

    with ThreadPoolExecutor(max_workers=min(len(country_codes), 8)) as pool:
        futures = {
            pool.submit(_check_country_availability, code, year, month): code
            for code in country_codes
        }
        for future in as_completed(futures):
            code, avail = future.result()
            results[code] = avail

    return DataAvailabilityResponse(period=period, countries=results)


# -- Last completed sync per country ----------------------------------------


class LastSyncEntry(BaseModel):
    country: str
    completed_at: str
    job_id: str
    stage: str


class LastSyncResponse(BaseModel):
    countries: dict[str, LastSyncEntry]


def _get_db_path(request: Request) -> str | None:
    """Get SQLite database path from app.state."""
    return getattr(request.app.state, "sqlite_db_path", None)


@router.get(
    "/last-sync",
    response_model=LastSyncResponse,
    summary="Last completed sync per country",
    description="Returns the most recent completed extraction job for each country.",
)
async def last_sync(
    request: Request,
    stage: str | None = Query(default=None, description="Filter by stage (e.g. 'calibracion')"),
    min_queries: int = Query(default=0, ge=0, description="Minimum number of queries in the job"),
) -> LastSyncResponse:
    """Return the last completed job for each country from the local SQLite store."""
    db_path = _get_db_path(request)
    if db_path is None:
        logger.warning("last-sync called but SQLite store is not initialised")
        raise HTTPException(status_code=503, detail="Local job store not available")

    rows = local_store.get_last_completed_per_country(db_path, stage=stage, min_queries=min_queries)
    countries: dict[str, LastSyncEntry] = {
        row["country"]: LastSyncEntry(
            country=row["country"],
            completed_at=row["completed_at"],
            job_id=row["job_id"],
            stage=row["stage"],
        )
        for row in rows
    }
    return LastSyncResponse(countries=countries)


@router.get(
    "/last-calibration",
    response_model=LastSyncResponse,
    summary="Last successful calibration per country",
    description="Returns the most recent completed calibration job for each country.",
)
async def last_calibration(request: Request) -> LastSyncResponse:
    """Return the last completed calibration job for each country."""
    db_path = _get_db_path(request)
    if db_path is None:
        logger.warning("last-calibration called but SQLite store is not initialised")
        raise HTTPException(status_code=503, detail="Local job store not available")

    rows = local_store.get_last_completed_per_country_by_stage(db_path, "calibracion")
    countries: dict[str, LastSyncEntry] = {
        row["country"]: LastSyncEntry(
            country=row["country"],
            completed_at=row["completed_at"],
            job_id=row["job_id"],
            stage=row["stage"],
        )
        for row in rows
    }
    return LastSyncResponse(countries=countries)


class LastElegibilidadEntry(BaseModel):
    country: str
    completed_at: str
    run_id: str
    period: int


class LastElegibilidadResponse(BaseModel):
    countries: dict[str, LastElegibilidadEntry]


def _get_eligibility_db_path(request: Request) -> str | None:
    """Resolve the eligibility SQLite DB path from app state."""
    import os

    sqlite_db_path = getattr(request.app.state, "sqlite_db_path", None)
    if sqlite_db_path:
        return os.path.join(os.path.dirname(sqlite_db_path), "eligibility.db")
    return None


@router.get(
    "/last-elegibilidad",
    response_model=LastElegibilidadResponse,
    summary="Last finalized elegibilidad per country",
    description="Returns the most recent finalized elegibilidad run for each country.",
)
async def last_elegibilidad(request: Request) -> LastElegibilidadResponse:
    """Return the last finalized elegibilidad run for each country."""
    db_path = _get_eligibility_db_path(request)
    if db_path is None:
        logger.warning("last-elegibilidad called but SQLite store is not initialised")
        raise HTTPException(status_code=503, detail="Local job store not available")

    rows = eligibility_store.get_last_finalized_per_country(db_path)
    countries: dict[str, LastElegibilidadEntry] = {
        row["country"]: LastElegibilidadEntry(
            country=row["country"],
            completed_at=row["completed_at"],
            run_id=row["run_id"],
            period=row["period"],
        )
        for row in rows
    }
    return LastElegibilidadResponse(countries=countries)
