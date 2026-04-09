"""Diff-sync schedule API — start/stop/status/history for periodic diff sync."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from sql_databricks_bridge.core.diff_sync_runner import (
    CountryRoundResult,
    append_results_csv,
    discover_countries,
    init_csv,
    run_diff_sync_round,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/diff-sync", tags=["Diff Sync"])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class DiffSyncScheduleRequest(BaseModel):
    countries: list[str] | None = Field(
        default=None,
        description="Countries to sync. null = all discovered countries.",
    )
    interval_minutes: int = Field(default=15, ge=1, description="Minutes between rounds")
    stage: str = Field(default="sincronizacion", description="Stage code")
    lookback_months: int = Field(default=24, ge=1, description="Rolling lookback months")
    log_file: str = Field(
        default="diff_sync_validation.csv",
        description="CSV log file path",
    )
    deferred_phase2: bool = Field(
        default=False,
        description=(
            "When True, each country runs Phase 1 only (extract + upload). "
            "After all countries finish, a single Phase 2 batch drains the queue."
        ),
    )


class TableResultResponse(BaseModel):
    table: str
    status: str
    rows_extracted: int = 0
    duration_s: float = 0.0
    error: str = ""


class CountryResultResponse(BaseModel):
    country: str
    job_id: str = ""
    status: str = "pending"
    tables: list[TableResultResponse] = []
    total_duration_s: float = 0.0
    error: str = ""


class DiffSyncStatusResponse(BaseModel):
    running: bool
    countries: list[str] = []
    interval_minutes: int = 0
    stage: str = ""
    lookback_months: int = 0
    round_number: int = 0
    last_round_at: str | None = None
    next_round_at: str | None = None
    last_results: list[CountryResultResponse] = []


class DiffSyncHistoryEntry(BaseModel):
    round_number: int
    timestamp: str
    results: list[CountryResultResponse]


class DiffSyncHistoryResponse(BaseModel):
    rounds: list[DiffSyncHistoryEntry]


# ---------------------------------------------------------------------------
# Singleton schedule manager
# ---------------------------------------------------------------------------


class _DiffSyncScheduleManager:
    """Manages a single background diff-sync loop."""

    def __init__(self) -> None:
        self._running = False
        self._task: asyncio.Task | None = None
        self._countries: list[str] = []
        self._interval: int = 15
        self._stage: str = "sincronizacion"
        self._lookback: int = 24
        self._log_file: str = "diff_sync_validation.csv"
        self._api_base: str = "http://127.0.0.1:8000/api/v1"
        self._deferred_phase2: bool = False
        self._round_number: int = 0
        self._last_round_at: datetime | None = None
        self._next_round_at: datetime | None = None
        self._last_results: list[CountryRoundResult] = []
        self._history: list[tuple[int, str, list[CountryRoundResult]]] = []

    @property
    def running(self) -> bool:
        return self._running

    def start(
        self,
        countries: list[str],
        interval: int,
        stage: str,
        lookback: int,
        log_file: str,
        api_base: str,
        deferred_phase2: bool = False,
    ) -> None:
        if self._running:
            raise RuntimeError("Already running")

        self._countries = countries
        self._interval = interval
        self._stage = stage
        self._lookback = lookback
        self._log_file = log_file
        self._api_base = api_base
        self._deferred_phase2 = deferred_phase2
        self._round_number = 0
        self._history = []
        self._last_results = []
        self._running = True

        init_csv(self._log_file)
        self._task = asyncio.create_task(self._loop())
        logger.info(
            "Diff-sync schedule started: countries=%s interval=%dmin",
            self._countries,
            self._interval,
        )

    def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        logger.info("Diff-sync schedule stopped")

    def get_status(self) -> DiffSyncStatusResponse:
        return DiffSyncStatusResponse(
            running=self._running,
            countries=self._countries,
            interval_minutes=self._interval,
            stage=self._stage,
            lookback_months=self._lookback,
            round_number=self._round_number,
            last_round_at=(
                self._last_round_at.isoformat(timespec="seconds")
                if self._last_round_at
                else None
            ),
            next_round_at=(
                self._next_round_at.isoformat(timespec="seconds")
                if self._next_round_at
                else None
            ),
            last_results=_to_response(self._last_results),
        )

    def get_history(self) -> DiffSyncHistoryResponse:
        entries = []
        for rn, ts, results in self._history:
            entries.append(
                DiffSyncHistoryEntry(
                    round_number=rn,
                    timestamp=ts,
                    results=_to_response(results),
                )
            )
        return DiffSyncHistoryResponse(rounds=entries)

    async def _loop(self) -> None:
        try:
            while self._running:
                self._round_number += 1
                ts = datetime.now().isoformat(timespec="seconds")
                logger.info("Diff-sync round #%d starting", self._round_number)

                results = await asyncio.to_thread(
                    run_diff_sync_round,
                    self._api_base,
                    self._countries,
                    self._stage,
                    self._lookback,
                    10,  # poll_interval
                    None,  # on_progress
                    None,  # table_suffix
                    False,  # all_tables
                    None,  # suffix_exclude
                    self._deferred_phase2,
                )

                self._last_results = results
                self._last_round_at = datetime.now()
                self._history.append((self._round_number, ts, results))

                # Keep only last 50 rounds in memory
                if len(self._history) > 50:
                    self._history = self._history[-50:]

                append_results_csv(self._log_file, self._round_number, results)

                logger.info(
                    "Diff-sync round #%d done: %d countries",
                    self._round_number,
                    len(results),
                )

                self._next_round_at = datetime.now() + timedelta(
                    minutes=self._interval
                )
                await asyncio.sleep(self._interval * 60)
        except asyncio.CancelledError:
            logger.info("Diff-sync loop cancelled")
        except Exception:
            logger.exception("Diff-sync loop error")
        finally:
            self._running = False
            self._next_round_at = None


# Module-level singleton
_manager = _DiffSyncScheduleManager()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_response(results: list[CountryRoundResult]) -> list[CountryResultResponse]:
    return [
        CountryResultResponse(
            country=cr.country,
            job_id=cr.job_id,
            status=cr.status,
            tables=[
                TableResultResponse(
                    table=t.table,
                    status=t.status,
                    rows_extracted=t.rows_extracted,
                    duration_s=t.duration_s,
                    error=t.error,
                )
                for t in cr.tables
            ],
            total_duration_s=cr.total_duration_s,
            error=cr.error,
        )
        for cr in results
    ]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/start",
    response_model=DiffSyncStatusResponse,
    summary="Start periodic diff sync",
    description=(
        "Launch a background loop that runs diff sync for the"
        " specified (or all) countries at a given interval."
    ),
)
async def start_diff_sync(req: DiffSyncScheduleRequest) -> DiffSyncStatusResponse:
    if _manager.running:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Diff-sync schedule is already running. Stop it first.",
        )

    # Resolve countries
    api_base = "http://127.0.0.1:8000/api/v1"
    if req.countries:
        countries = req.countries
    else:
        try:
            countries = discover_countries(api_base)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Failed to discover countries: {e}",
            ) from e

    _manager.start(
        countries=countries,
        interval=req.interval_minutes,
        stage=req.stage,
        lookback=req.lookback_months,
        log_file=req.log_file,
        api_base=api_base,
        deferred_phase2=req.deferred_phase2,
    )

    return _manager.get_status()


@router.post(
    "/stop",
    response_model=DiffSyncStatusResponse,
    summary="Stop periodic diff sync",
)
async def stop_diff_sync() -> DiffSyncStatusResponse:
    if not _manager.running:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Diff-sync schedule is not running.",
        )
    _manager.stop()
    return _manager.get_status()


@router.get(
    "/status",
    response_model=DiffSyncStatusResponse,
    summary="Get diff-sync schedule status",
)
async def diff_sync_status() -> DiffSyncStatusResponse:
    return _manager.get_status()


@router.get(
    "/history",
    response_model=DiffSyncHistoryResponse,
    summary="Get past diff-sync round results",
)
async def diff_sync_history() -> DiffSyncHistoryResponse:
    return _manager.get_history()
