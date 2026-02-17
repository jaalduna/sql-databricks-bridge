"""Metadata API endpoints -- country, query, and stage discovery."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from fastapi import APIRouter, Query
from pydantic import BaseModel

from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.core.country_query_loader import CountryAwareQueryLoader
from sql_databricks_bridge.core.stages import load_stages
from sql_databricks_bridge.db.sql_server import SQLServerClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/metadata", tags=["Metadata"])


class CountryInfo(BaseModel):
    code: str
    queries: list[str]
    queries_count: int


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
    """List all available countries and their queries."""
    queries_base = Path(get_settings().queries_path)
    countries_path = queries_base / "countries"

    if not countries_path.exists():
        return CountriesResponse(countries=[])

    loader = CountryAwareQueryLoader(queries_base)
    result = []

    for country_dir in sorted(countries_path.iterdir()):
        if not country_dir.is_dir() or country_dir.name.startswith("."):
            continue

        country_code = country_dir.name
        queries = loader.list_queries(country_code)

        result.append(
            CountryInfo(
                code=country_code,
                queries=queries,
                queries_count=len(queries),
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


def _check_country_availability(country: str, year: int) -> tuple[str, CountryAvailability]:
    """Check pesaje & elegibilidad tables for *country* on SQL Server.

    Runs synchronously (blocking I/O) – intended to be called inside a
    ThreadPoolExecutor so multiple countries are checked in parallel.
    """
    try:
        client = SQLServerClient(country=country)

        # Pesaje check – if pesaje exists, elegibilidad is implied
        pesaje_df = client.execute_query(
            f"SELECT TOP 1 1 AS flag FROM rg_domicilios_pesos WHERE ano = {year}"
        )
        has_pesaje = len(pesaje_df) > 0

        if has_pesaje:
            return country, CountryAvailability(elegibilidad=True, pesaje=True)

        # Elegibilidad check (only when pesaje is absent)
        eleg_df = client.execute_query(
            f"SELECT TOP 1 1 AS flag FROM mordom WHERE ano = {year}"
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

    year = int(period[:4])
    results: dict[str, CountryAvailability] = {}

    if not country_codes:
        return DataAvailabilityResponse(period=period, countries=results)

    with ThreadPoolExecutor(max_workers=min(len(country_codes), 8)) as pool:
        futures = {
            pool.submit(_check_country_availability, code, year): code
            for code in country_codes
        }
        for future in as_completed(futures):
            code, avail = future.result()
            results[code] = avail

    return DataAvailabilityResponse(period=period, countries=results)
