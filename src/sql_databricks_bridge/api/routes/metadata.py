"""Metadata API endpoints -- country, query, and stage discovery."""

import logging
from pathlib import Path

from fastapi import APIRouter
from pydantic import BaseModel

from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.core.country_query_loader import CountryAwareQueryLoader
from sql_databricks_bridge.core.stages import load_stages

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
