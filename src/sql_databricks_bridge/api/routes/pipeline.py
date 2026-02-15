"""Calibration pipeline status API endpoints.

Provides endpoints for the calibration-frontend to track the full
calibration pipeline: sync → copy_bronze → merge → simulate_weights →
simulate_kpis → calculate_targets.
"""

import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from sql_databricks_bridge.api.dependencies import CurrentAzureADUser
from sql_databricks_bridge.core.pipeline_tracker import pipeline_tracker
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.models.pipeline import (
    CalibrationPipeline,
    StepStatus,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pipeline", tags=["Pipeline"])


# --- Request / Response schemas ---


class CreatePipelineRequest(BaseModel):
    country: str = Field(..., description="Country code (e.g. 'bolivia')")
    period: str | None = Field(default=None, description="Period code (e.g. '202602')")
    sync_queries: list[str] | None = Field(
        default=None, description="Queries for the sync step (null = use defaults)"
    )


class SubStepResponse(BaseModel):
    name: str
    status: StepStatus
    started_at: datetime | None = None
    completed_at: datetime | None = None
    rows_affected: int | None = None
    error: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class StepResponse(BaseModel):
    step_id: str
    name: str
    order: int
    status: StepStatus
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_seconds: float | None = None
    error: str | None = None
    substeps: list[SubStepResponse] = Field(default_factory=list)
    substeps_total: int = 0
    substeps_completed: int = 0
    substeps_failed: int = 0
    progress_pct: float = 0.0
    databricks_run_id: int | None = None


class PipelineResponse(BaseModel):
    pipeline_id: str
    country: str
    period: str | None = None
    status: StepStatus
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    triggered_by: str = ""
    steps: list[StepResponse]
    error: str | None = None
    steps_completed: int = 0
    progress_pct: float = 0.0
    current_step: str | None = None


class PipelineListResponse(BaseModel):
    items: list[PipelineResponse]
    total: int
    limit: int
    offset: int


class UpdateStepRequest(BaseModel):
    status: StepStatus
    error: str | None = None
    databricks_run_id: int | None = None


class UpdateSubStepRequest(BaseModel):
    status: StepStatus
    rows_affected: int | None = None
    error: str | None = None


# --- Helpers ---


def _pipeline_to_response(p: CalibrationPipeline) -> PipelineResponse:
    """Convert a CalibrationPipeline model to the API response shape."""
    steps = []
    for s in p.steps:
        substeps = [
            SubStepResponse(
                name=ss.name,
                status=ss.status,
                started_at=ss.started_at,
                completed_at=ss.completed_at,
                rows_affected=ss.rows_affected,
                error=ss.error,
                metadata=ss.metadata,
            )
            for ss in s.substeps
        ]
        steps.append(
            StepResponse(
                step_id=s.step_id,
                name=s.name,
                order=s.order,
                status=s.status,
                started_at=s.started_at,
                completed_at=s.completed_at,
                duration_seconds=s.duration_seconds,
                error=s.error,
                substeps=substeps,
                substeps_total=s.substeps_total,
                substeps_completed=s.substeps_completed,
                substeps_failed=s.substeps_failed,
                progress_pct=s.progress_pct,
                databricks_run_id=s.databricks_run_id,
            )
        )

    current = p.current_step
    return PipelineResponse(
        pipeline_id=p.pipeline_id,
        country=p.country,
        period=p.period,
        status=p.status,
        created_at=p.created_at,
        started_at=p.started_at,
        completed_at=p.completed_at,
        triggered_by=p.triggered_by,
        steps=steps,
        error=p.error,
        steps_completed=p.steps_completed,
        progress_pct=p.progress_pct,
        current_step=current.step_id if current else None,
    )


def _get_databricks_client(request: Request) -> DatabricksClient:
    client = getattr(request.app.state, "databricks_client", None)
    if client is not None:
        return client
    return DatabricksClient()


# --- Endpoints ---


@router.post(
    "",
    response_model=PipelineResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create calibration pipeline",
    description="Create a new calibration pipeline run for a country.",
)
async def create_pipeline(
    body: CreatePipelineRequest,
    user: CurrentAzureADUser,
) -> PipelineResponse:
    if not user.can_trigger_sync:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"error": "forbidden", "message": "User role does not allow pipeline creation"},
        )
    if not user.can_access_country(body.country):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"error": "forbidden", "message": f"User not authorized for country '{body.country}'"},
        )

    pipeline = pipeline_tracker.create_pipeline(
        country=body.country,
        period=body.period,
        triggered_by=user.email,
        sync_queries=body.sync_queries,
    )
    logger.info(f"Created pipeline {pipeline.pipeline_id} for {body.country}")
    return _pipeline_to_response(pipeline)


@router.get(
    "",
    response_model=PipelineListResponse,
    summary="List calibration pipelines",
)
async def list_pipelines(
    user: CurrentAzureADUser,
    country: str | None = Query(default=None),
    pipeline_status: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> PipelineListResponse:
    items, total = pipeline_tracker.list_pipelines(
        country=country,
        status=pipeline_status,
        limit=limit,
        offset=offset,
    )
    return PipelineListResponse(
        items=[_pipeline_to_response(p) for p in items],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/{pipeline_id}",
    response_model=PipelineResponse,
    summary="Get pipeline detail",
)
async def get_pipeline(
    pipeline_id: str,
    user: CurrentAzureADUser,
) -> PipelineResponse:
    pipeline = pipeline_tracker.get_pipeline(pipeline_id)
    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Pipeline not found: {pipeline_id}"},
        )
    return _pipeline_to_response(pipeline)


@router.post(
    "/{pipeline_id}/poll",
    response_model=PipelineResponse,
    summary="Poll Databricks for step updates",
    description="Poll Databricks Jobs API for steps with attached run IDs and update status.",
)
async def poll_pipeline(
    pipeline_id: str,
    user: CurrentAzureADUser,
    raw_request: Request,
) -> PipelineResponse:
    pipeline = pipeline_tracker.get_pipeline(pipeline_id)
    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Pipeline not found: {pipeline_id}"},
        )

    client = _get_databricks_client(raw_request)
    updates = pipeline_tracker.poll_databricks_status(pipeline_id, client)
    if updates:
        logger.info(f"Pipeline {pipeline_id}: polled {len(updates)} step updates")

    return _pipeline_to_response(pipeline)


@router.patch(
    "/{pipeline_id}/steps/{step_id}",
    response_model=PipelineResponse,
    summary="Update a pipeline step",
    description="Update the status of a specific pipeline step.",
)
async def update_step(
    pipeline_id: str,
    step_id: str,
    body: UpdateStepRequest,
    user: CurrentAzureADUser,
) -> PipelineResponse:
    pipeline = pipeline_tracker.get_pipeline(pipeline_id)
    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Pipeline not found: {pipeline_id}"},
        )

    if body.databricks_run_id is not None:
        pipeline_tracker.set_step_run_id(pipeline_id, step_id, body.databricks_run_id)

    if body.status == StepStatus.RUNNING:
        step = pipeline_tracker.start_step(pipeline_id, step_id)
    elif body.status in (StepStatus.COMPLETED, StepStatus.FAILED):
        step = pipeline_tracker.complete_step(pipeline_id, step_id, error=body.error)
    elif body.status == StepStatus.SKIPPED:
        step = pipeline_tracker.skip_step(pipeline_id, step_id)
    else:
        step = None

    if not step:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Step not found: {step_id}"},
        )

    return _pipeline_to_response(pipeline)


@router.patch(
    "/{pipeline_id}/steps/{step_id}/substeps/{substep_name}",
    response_model=PipelineResponse,
    summary="Update a substep",
    description="Update the status of a specific substep within a step.",
)
async def update_substep(
    pipeline_id: str,
    step_id: str,
    substep_name: str,
    body: UpdateSubStepRequest,
    user: CurrentAzureADUser,
) -> PipelineResponse:
    pipeline = pipeline_tracker.get_pipeline(pipeline_id)
    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Pipeline not found: {pipeline_id}"},
        )

    ss = pipeline_tracker.update_substep(
        pipeline_id, step_id, substep_name,
        status=body.status,
        rows_affected=body.rows_affected,
        error=body.error,
    )
    if not ss:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "not_found",
                "message": f"Substep '{substep_name}' not found in step '{step_id}'",
            },
        )

    return _pipeline_to_response(pipeline)


@router.delete(
    "/{pipeline_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete pipeline",
)
async def delete_pipeline(
    pipeline_id: str,
    user: CurrentAzureADUser,
) -> None:
    if not user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"error": "forbidden", "message": "Only admins can delete pipelines"},
        )
    if not pipeline_tracker.delete_pipeline(pipeline_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Pipeline not found: {pipeline_id}"},
        )
