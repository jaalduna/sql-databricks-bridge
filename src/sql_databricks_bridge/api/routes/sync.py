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
    description="Get the status of the background event poller.",
)
async def get_poller_status() -> dict:
    """Get event poller status."""
    from sql_databricks_bridge.main import _event_poller

    if _event_poller is None:
        return {
            "enabled": False,
            "running": False,
            "message": "Event poller not configured (no warehouse_id)",
        }

    return {
        "enabled": True,
        "running": _event_poller.is_running,
        "events_table": _event_poller.events_table,
        "poll_interval_seconds": _event_poller.poll_interval,
        "max_events_per_poll": _event_poller.max_events_per_poll,
    }


@router.post(
    "/poller/trigger",
    summary="Trigger poll cycle",
    description="Manually trigger a poll cycle to process pending events.",
)
async def trigger_poll_cycle() -> dict:
    """Manually trigger a poll cycle."""
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
