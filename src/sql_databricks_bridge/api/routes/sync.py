"""Sync API endpoints for Databricks → SQL Server operations."""

import asyncio
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect, status

from sql_databricks_bridge.api.schemas import (
    SyncEvent,
    SyncEventOperation,
    SyncEventStatus,
)
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.sql_server import SQLServerClient
from sql_databricks_bridge.models.events import SyncEvent as SyncEventModel
from sql_databricks_bridge.models.events import SyncOperation, SyncStatus
from sql_databricks_bridge.sync.operations import SyncOperator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sync", tags=["Sync"])

# In-memory event storage (would use Redis or DB in production)
_sync_events: dict[str, SyncEvent] = {}
_event_subscribers: list[WebSocket] = []


# --- Dependency injection ---


def get_sync_operator(country: str) -> SyncOperator:
    """Create sync operator with clients for specific country."""
    sql_client = SQLServerClient(country=country)
    databricks_client = DatabricksClient()
    return SyncOperator(
        sql_client=sql_client,
        databricks_client=databricks_client,
        max_delete_rows=10000,
    )


# --- Helper functions ---


async def broadcast_event_update(event: SyncEvent) -> None:
    """Broadcast event update to all WebSocket subscribers."""
    if not _event_subscribers:
        return

    message = event.model_dump_json()
    disconnected = []

    for ws in _event_subscribers:
        try:
            await ws.send_text(message)
        except Exception:
            disconnected.append(ws)

    # Remove disconnected clients
    for ws in disconnected:
        _event_subscribers.remove(ws)


def api_event_to_model(event: SyncEvent) -> SyncEventModel:
    """Convert API SyncEvent to internal model."""
    return SyncEventModel(
        event_id=event.event_id,
        operation=SyncOperation(event.operation.value),
        source_table=event.source_table,
        target_table=event.target_table,
        primary_keys=event.primary_keys,
        priority=event.priority,
        status=SyncStatus(event.status.value),
        rows_expected=event.rows_expected,
        filter_conditions=event.metadata.get("filter_conditions", {}),
        metadata=event.metadata,
    )


# --- REST Endpoints ---


@router.post(
    "/events",
    response_model=SyncEvent,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit sync event",
    description="Submit a new sync event for processing (Databricks → SQL Server).",
)
async def submit_sync_event(
    event: SyncEvent,
    process_immediately: bool = Query(
        default=False,
        description="Process event immediately instead of queuing",
    ),
) -> SyncEvent:
    """Submit a new sync event."""
    # Validate operation requires primary keys
    if event.operation in (SyncEventOperation.UPDATE, SyncEventOperation.DELETE):
        if not event.primary_keys:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Primary keys required for {event.operation.value} operation",
            )

    # Store event
    _sync_events[event.event_id] = event
    logger.info(
        f"Received sync event {event.event_id}: {event.operation.value} for country {event.country}"
    )

    if process_immediately:
        # Create country-specific operator
        operator = get_sync_operator(event.country)

        # Process synchronously
        event.status = SyncEventStatus.PROCESSING
        await broadcast_event_update(event)

        try:
            model_event = api_event_to_model(event)
            result = await operator.process_event(model_event)

            if result.success:
                event.status = SyncEventStatus.COMPLETED
                event.rows_affected = result.rows_affected
                event.discrepancy = result.discrepancy
                event.warning = result.warning
            else:
                event.status = SyncEventStatus.FAILED
                event.error_message = result.error

        except Exception as e:
            event.status = SyncEventStatus.FAILED
            event.error_message = str(e)
            logger.exception(f"Failed to process event {event.event_id}: {e}")

        await broadcast_event_update(event)

    return event


@router.get(
    "/events",
    response_model=list[SyncEvent],
    summary="List sync events",
    description="List sync events with optional filtering.",
)
async def list_sync_events(
    status_filter: SyncEventStatus | None = Query(
        default=None,
        description="Filter by event status",
    ),
    operation_filter: SyncEventOperation | None = Query(
        default=None,
        description="Filter by operation type",
    ),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> list[SyncEvent]:
    """List sync events with optional filters."""
    events = list(_sync_events.values())

    # Apply filters
    if status_filter:
        events = [e for e in events if e.status == status_filter]

    if operation_filter:
        events = [e for e in events if e.operation == operation_filter]

    # Sort by created_at (newest first), handle None
    events.sort(
        key=lambda e: e.created_at if e.created_at else "",
        reverse=True,
    )

    # Apply pagination
    return events[offset : offset + limit]


@router.get(
    "/events/{event_id}",
    response_model=SyncEvent,
    summary="Get sync event",
    description="Get details of a specific sync event.",
)
async def get_sync_event(event_id: str) -> SyncEvent:
    """Get a sync event by ID."""
    event = _sync_events.get(event_id)

    if not event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Event not found: {event_id}",
        )

    return event


@router.post(
    "/events/{event_id}/retry",
    response_model=SyncEvent,
    summary="Retry failed event",
    description="Retry a failed or blocked sync event.",
)
async def retry_sync_event(event_id: str) -> SyncEvent:
    """Retry a failed sync event."""
    event = _sync_events.get(event_id)

    if not event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Event not found: {event_id}",
        )

    if event.status not in (SyncEventStatus.FAILED, SyncEventStatus.BLOCKED):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot retry event with status: {event.status.value}",
        )

    # Create country-specific operator
    operator = get_sync_operator(event.country)

    # Reset and reprocess
    event.status = SyncEventStatus.PROCESSING
    event.error_message = None
    event.warning = None
    await broadcast_event_update(event)

    try:
        model_event = api_event_to_model(event)
        result = await operator.process_event(model_event)

        if result.success:
            event.status = SyncEventStatus.COMPLETED
            event.rows_affected = result.rows_affected
            event.discrepancy = result.discrepancy
            event.warning = result.warning
        else:
            event.status = SyncEventStatus.FAILED
            event.error_message = result.error

    except Exception as e:
        event.status = SyncEventStatus.FAILED
        event.error_message = str(e)
        logger.exception(f"Failed to retry event {event_id}: {e}")

    await broadcast_event_update(event)
    return event


@router.delete(
    "/events/{event_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Cancel sync event",
    description="Cancel a pending sync event.",
)
async def cancel_sync_event(event_id: str) -> None:
    """Cancel a pending sync event."""
    event = _sync_events.get(event_id)

    if not event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Event not found: {event_id}",
        )

    if event.status != SyncEventStatus.PENDING:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot cancel event with status: {event.status.value}",
        )

    # Remove from queue
    del _sync_events[event_id]
    logger.info(f"Cancelled sync event {event_id}")


@router.get(
    "/stats",
    summary="Get sync statistics",
    description="Get statistics about sync events.",
)
async def get_sync_stats() -> dict:
    """Get sync event statistics."""
    events = list(_sync_events.values())

    stats = {
        "total": len(events),
        "by_status": {},
        "by_operation": {},
    }

    for status_val in SyncEventStatus:
        count = sum(1 for e in events if e.status == status_val)
        if count > 0:
            stats["by_status"][status_val.value] = count

    for op_val in SyncEventOperation:
        count = sum(1 for e in events if e.operation == op_val)
        if count > 0:
            stats["by_operation"][op_val.value] = count

    return stats


# --- Poller Status Endpoints ---


@router.get(
    "/poller/status",
    summary="Get poller status",
    description="Get the status of the event poller and its schedule.",
)
async def get_poller_status() -> dict:
    """Get event poller status."""
    from sql_databricks_bridge.main import _event_poller

    if _event_poller is None:
        return {
            "enabled": False,
            "message": "Event poller not configured (no warehouse_id)",
        }

    # Check schedule from schedules.yaml
    schedule_info: dict = {"mode": "on-demand"}
    try:
        from sql_databricks_bridge.core.config import get_settings
        from sql_databricks_bridge.core.scheduler import load_schedules
        settings = get_settings()
        schedules = load_schedules(settings.schedules_file)
        ep = schedules.get("event_poller")
        if ep and ep.enabled:
            schedule_info = {
                "mode": "scheduled",
                "hours_utc": ep.hours,
                "minute": ep.minute,
                "weekdays_only": ep.weekdays_only,
            }
    except Exception:
        pass

    return {
        "enabled": True,
        "events_table": _event_poller.events_table,
        "max_events_per_poll": _event_poller.max_events_per_poll,
        "schedule": schedule_info,
    }


@router.post(
    "/poller/trigger",
    summary="Trigger poll cycle",
    description="Run a single poll cycle: query all pending events and process them.",
)
async def trigger_poll_cycle() -> dict:
    """Manually trigger a single poll cycle."""
    from sql_databricks_bridge.main import _event_poller

    if _event_poller is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event poller not configured",
        )

    try:
        events_processed = await _event_poller.poll_cycle()
        return {
            "success": True,
            "events_processed": events_processed,
        }
    except Exception as e:
        logger.exception(f"Manual poll cycle failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        )


# --- Recovery: reset stuck 'processing' events ---


def _build_dbx_client() -> DatabricksClient:
    """Build a DatabricksClient, preferring the poller warehouse when set.

    Keeps recovery queries isolated from diff-sync/trigger warehouse
    traffic, matching the poller's own isolation.
    """
    from sql_databricks_bridge.core.config import get_settings

    settings = get_settings().databricks
    if settings.poller_warehouse_id and (
        settings.poller_warehouse_id != settings.warehouse_id
    ):
        settings = settings.model_copy(
            update={"warehouse_id": settings.poller_warehouse_id}
        )
    return DatabricksClient(settings=settings)


@router.post(
    "/events/recover-stuck-processing",
    summary="Reset stuck 'processing' events back to 'pending'",
    description=(
        "Scan bridge_events for rows still in 'processing' older than "
        "*older_than_minutes*. If *dry_run=true* (default), returns the list "
        "without modifying anything.  Otherwise resets them to 'pending' so "
        "the next poll cycle picks them up.  **WARNING**: this can cause "
        "re-execution of operations that already succeeded in SQL Server — "
        "only safe for idempotent operations or when the on-call operator "
        "has verified the source work is incomplete."
    ),
)
async def recover_stuck_processing_events(
    older_than_minutes: int = Query(
        default=10,
        ge=1,
        le=1440,
        description="Only consider events whose processed_at is older than this",
    ),
    limit: int = Query(default=100, ge=1, le=1000),
    dry_run: bool = Query(
        default=True,
        description="If true, list candidates without modifying.",
    ),
) -> dict:
    """Reset rows stuck in 'processing' state older than the cutoff."""
    from sql_databricks_bridge.core.config import get_settings

    events_table = get_settings().events_table
    client = _build_dbx_client()

    # Candidates = processing AND processed_at older than cutoff (or NULL,
    # meaning we never even recorded a start — also suspicious).
    select_sql = (
        f"SELECT event_id, source_table, target_table, processed_at "
        f"FROM {events_table} "
        f"WHERE status = 'processing' "
        f"  AND (processed_at IS NULL "
        f"       OR processed_at < current_timestamp() - INTERVAL {int(older_than_minutes)} MINUTES) "
        f"ORDER BY processed_at NULLS FIRST "
        f"LIMIT {int(limit)}"
    )

    try:
        candidates = await asyncio.to_thread(client.execute_sql, select_sql)
    except Exception as e:
        logger.exception("recover: candidate query failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"failed to query stuck events: {e}",
        )

    if not candidates:
        return {
            "dry_run": dry_run,
            "older_than_minutes": older_than_minutes,
            "candidates": 0,
            "reset": 0,
            "event_ids": [],
        }

    ids = [str(r["event_id"]) for r in candidates]

    if dry_run:
        return {
            "dry_run": True,
            "older_than_minutes": older_than_minutes,
            "candidates": len(ids),
            "reset": 0,
            "event_ids": ids,
        }

    # Escape single quotes and build the UPDATE
    escaped = ",".join("'" + i.replace("'", "''") + "'" for i in ids)
    update_sql = (
        f"UPDATE {events_table} "
        f"SET status = 'pending', error_message = NULL, warning = NULL "
        f"WHERE event_id IN ({escaped}) AND status = 'processing'"
    )

    try:
        await asyncio.to_thread(client.execute_sql, update_sql)
    except Exception as e:
        logger.exception("recover: reset UPDATE failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"failed to reset events: {e}",
        )

    logger.warning(
        "recover: reset %d stuck processing events to pending | ids=%s",
        len(ids), ids[:10],
    )

    return {
        "dry_run": False,
        "older_than_minutes": older_than_minutes,
        "candidates": len(ids),
        "reset": len(ids),
        "event_ids": ids,
    }


# --- WebSocket Endpoint for Real-time Updates ---


@router.websocket("/events/ws")
async def websocket_event_stream(websocket: WebSocket) -> None:
    """WebSocket endpoint for real-time event updates.

    Clients connect to receive live updates when events are processed.
    Send 'ping' to keep connection alive, receive 'pong' back.
    """
    await websocket.accept()
    _event_subscribers.append(websocket)
    logger.info(f"WebSocket client connected. Total subscribers: {len(_event_subscribers)}")

    try:
        while True:
            # Wait for messages (ping/pong or commands)
            data = await websocket.receive_text()

            if data == "ping":
                await websocket.send_text("pong")
            elif data == "list":
                # Send current events
                events = [e.model_dump() for e in _sync_events.values()]
                await websocket.send_json({"type": "event_list", "events": events})
            elif data.startswith("subscribe:"):
                # Subscribe to specific event
                event_id = data.split(":", 1)[1]
                event = _sync_events.get(event_id)
                if event:
                    await websocket.send_text(event.model_dump_json())
                else:
                    await websocket.send_json({"error": f"Event not found: {event_id}"})

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        if websocket in _event_subscribers:
            _event_subscribers.remove(websocket)
        logger.info(f"WebSocket cleanup. Remaining subscribers: {len(_event_subscribers)}")
