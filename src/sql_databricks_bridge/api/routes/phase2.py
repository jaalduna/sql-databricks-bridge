"""Phase 2 on-demand API — execute deferred Databricks writes in a single batch.

When ``skip_phase2=True`` is used during extraction (via the trigger API or
diff-sync runner), write operations are queued in the local SyncQueue but
**not** committed.  This router exposes endpoints to inspect and drain that
queue so a single SQL Warehouse wake-up covers all countries.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field

from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.core.delta_writer import DeltaTableWriter
from sql_databricks_bridge.core.fingerprint_cache import FingerprintCache
from sql_databricks_bridge.core.phase2_executor import Phase2Executor, Phase2Result
from sql_databricks_bridge.core.sync_queue import SyncQueue
from sql_databricks_bridge.db.databricks import DatabricksClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/phase2", tags=["Phase 2"])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class Phase2StatusItem(BaseModel):
    country: str
    table_name: str
    operation: str
    job_id: str


class Phase2StatusResponse(BaseModel):
    pending_count: int
    items: list[Phase2StatusItem]


class Phase2ExecuteRequest(BaseModel):
    job_id: str | None = Field(
        default=None,
        description="Drain only items for this job. null = drain ALL pending items.",
    )
    max_parallel: int = Field(
        default=4,
        ge=1,
        le=16,
        description="Max concurrent Databricks SQL operations.",
    )


class Phase2ExecuteResponse(BaseModel):
    tables_committed: int
    tables_failed: int
    total_rows: int
    fingerprints_saved: int
    duration_seconds: float
    errors: list[dict]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/status",
    response_model=Phase2StatusResponse,
    summary="Show pending Phase 2 queue items",
)
async def phase2_status() -> Phase2StatusResponse:
    settings = get_settings()
    if not settings.two_phase_sync:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="TWO_PHASE_SYNC is not enabled.",
        )

    queue = SyncQueue()
    pending = queue.get_pending()
    items = [
        Phase2StatusItem(
            country=item["country"],
            table_name=item["table_name"],
            operation=item["operation"],
            job_id=item["job_id"],
        )
        for item in pending
    ]
    return Phase2StatusResponse(pending_count=len(items), items=items)


@router.post(
    "/execute",
    response_model=Phase2ExecuteResponse,
    summary="Execute deferred Phase 2 (drain the SyncQueue)",
)
async def phase2_execute(
    raw_request: Request,
    req: Phase2ExecuteRequest | None = None,
) -> Phase2ExecuteResponse:
    settings = get_settings()
    if not settings.two_phase_sync:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="TWO_PHASE_SYNC is not enabled.",
        )

    queue = SyncQueue()
    pending_count = queue.count_pending(job_id=req.job_id if req else None)
    if pending_count == 0:
        return Phase2ExecuteResponse(
            tables_committed=0,
            tables_failed=0,
            total_rows=0,
            fingerprints_saved=0,
            duration_seconds=0,
            errors=[],
        )

    job_id = req.job_id if req else None
    max_parallel = req.max_parallel if req else 4

    logger.info(
        "Phase 2 execute: draining %d pending item(s) (job_id=%s, max_parallel=%d)",
        pending_count,
        job_id or "ALL",
        max_parallel,
    )

    # Build executor from server-side singletons / settings
    dbx_client = DatabricksClient()
    writer = DeltaTableWriter(dbx_client)
    fp_cache = FingerprintCache()

    executor = Phase2Executor(
        dbx_client=dbx_client,
        writer=writer,
        queue=queue,
        fingerprint_cache=fp_cache,
        fingerprint_table=settings.fingerprint_table,
    )

    result: Phase2Result = executor.execute_batch(
        job_id=job_id,
        max_parallel=max_parallel,
    )

    # Cleanup old entries after a successful batch
    try:
        queue.cleanup_old(days=7)
    except Exception:
        pass

    logger.info(
        "Phase 2 execute done: %d committed, %d failed, %d fp saves in %.1fs",
        result.tables_committed,
        result.tables_failed,
        result.fingerprints_saved,
        result.duration_seconds,
    )

    return Phase2ExecuteResponse(
        tables_committed=result.tables_committed,
        tables_failed=result.tables_failed,
        total_rows=result.total_rows,
        fingerprints_saved=result.fingerprints_saved,
        duration_seconds=result.duration_seconds,
        errors=result.errors,
    )
