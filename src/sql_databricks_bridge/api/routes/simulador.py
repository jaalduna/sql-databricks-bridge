"""Simulador sync API — trigger network-share-to-Databricks sync for product attributes & voleq."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.core.delta_writer import DeltaTableWriter
from sql_databricks_bridge.core.simulador_sync import (
    SimuladorSyncer,
    SyncRoundResult,
    _parse_period_folder,
    get_latest_period,
    parse_country_map,
)
from sql_databricks_bridge.db.databricks import DatabricksClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/simulador", tags=["Simulador"])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class SimuladorSyncRequest(BaseModel):
    countries: list[str] | None = Field(
        default=None,
        description="Country codes to sync (e.g. ['BO','CO']). null = all.",
    )
    period: str | None = Field(
        default=None,
        description="Period folder (e.g. '2026_02'). null = latest.",
    )
    data_types: list[str] | None = Field(
        default=None,
        description="Data types to sync: ['attributes','voleq']. null = both.",
    )
    dry_run: bool = Field(default=False, description="Parse only, no upload.")


class CountryResultResponse(BaseModel):
    country_code: str
    country_name: str
    data_type: str
    status: str
    rows: int = 0
    table_name: str = ""
    duration_s: float = 0.0
    error: str = ""


class SimuladorSyncResponse(BaseModel):
    period: str
    started_at: str = ""
    finished_at: str = ""
    dry_run: bool = False
    results: list[CountryResultResponse] = []


class SimuladorStatusResponse(BaseModel):
    running: bool
    last_result: SimuladorSyncResponse | None = None


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_running = False
_last_result: SyncRoundResult | None = None


def _to_response(rr: SyncRoundResult) -> SimuladorSyncResponse:
    return SimuladorSyncResponse(
        period=rr.period,
        started_at=rr.started_at,
        finished_at=rr.finished_at,
        dry_run=rr.dry_run,
        results=[
            CountryResultResponse(
                country_code=r.country_code,
                country_name=r.country_name,
                data_type=r.data_type,
                status=r.status,
                rows=r.rows,
                table_name=r.table_name,
                duration_s=r.duration_s,
                error=r.error,
            )
            for r in rr.results
        ],
    )


def _run_sync(req: SimuladorSyncRequest) -> SyncRoundResult:
    """Execute sync in a blocking thread."""
    global _running, _last_result
    settings = get_settings()
    base_path = Path(settings.simulador_base_path)
    country_map = parse_country_map(settings.simulador_countries)

    # Resolve period
    resolved_period = None
    if req.period:
        resolved_period = _parse_period_folder(req.period)
        if resolved_period is None:
            raise ValueError(f"Invalid period format: {req.period}")
    else:
        resolved_period = get_latest_period(base_path)
        if resolved_period is None:
            raise ValueError("No common period found in ATTRIBUTES and VOLEQ folders")

    # Create syncer
    if req.dry_run:
        writer = None  # type: ignore[arg-type]
    else:
        databricks_client = DatabricksClient()
        writer = DeltaTableWriter(databricks_client)

    syncer = SimuladorSyncer(writer=writer, base_path=base_path, country_map=country_map)

    result = syncer.sync_all(
        period=resolved_period,
        countries=req.countries,
        data_types=req.data_types,
        dry_run=req.dry_run,
    )
    _last_result = result
    return result


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/sync",
    response_model=SimuladorSyncResponse,
    summary="Trigger simulador data sync",
    description="Sync product attributes and/or voleq from network share to Databricks.",
)
async def sync_simulador(req: SimuladorSyncRequest) -> SimuladorSyncResponse:
    global _running

    if _running:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A simulador sync is already running.",
        )

    _running = True
    try:
        result = await asyncio.to_thread(_run_sync, req)
        return _to_response(result)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except Exception as e:
        logger.exception("Simulador sync failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        ) from e
    finally:
        _running = False


@router.get(
    "/status",
    response_model=SimuladorStatusResponse,
    summary="Get simulador sync status",
)
async def simulador_status() -> SimuladorStatusResponse:
    return SimuladorStatusResponse(
        running=_running,
        last_result=_to_response(_last_result) if _last_result else None,
    )
