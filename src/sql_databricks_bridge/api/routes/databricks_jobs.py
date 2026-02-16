"""Databricks Jobs API -- trigger and monitor Databricks jobs directly."""

import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field

from sql_databricks_bridge.api.dependencies import CurrentAzureADUser
from sql_databricks_bridge.db.databricks import DatabricksClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/databricks", tags=["Databricks Jobs"])


# --- Schemas ---


class RunJobRequest(BaseModel):
    parameters: dict[str, str] = Field(
        default_factory=dict,
        description="Job parameters (key-value pairs passed to job_parameters).",
    )


class RunJobResponse(BaseModel):
    job_id: int
    run_id: int
    message: str = "Job triggered successfully"


class TaskInfo(BaseModel):
    task_key: str | None = None
    run_id: int | None = None
    state: str | None = None
    result_state: str | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None


class RunStatusResponse(BaseModel):
    run_id: int
    job_id: int
    run_name: str | None = None
    state: str
    result_state: str | None = None
    state_message: str | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    run_duration_ms: int | None = None
    run_page_url: str | None = None
    tasks: list[TaskInfo] = Field(default_factory=list)


# --- Helpers ---


def _get_databricks_client(request: Request) -> DatabricksClient:
    client = getattr(request.app.state, "databricks_client", None)
    if client is not None:
        return client
    return DatabricksClient()


def _epoch_ms_to_datetime(epoch_ms: int | None) -> datetime | None:
    if epoch_ms is None or epoch_ms == 0:
        return None
    return datetime.utcfromtimestamp(epoch_ms / 1000)


# --- Endpoints ---


@router.post(
    "/jobs/{job_id}/run",
    response_model=RunJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger a Databricks job",
    description="Trigger an existing Databricks job by its numeric ID with optional parameters.",
)
async def run_databricks_job(
    job_id: int,
    body: RunJobRequest,
    user: CurrentAzureADUser,
    raw_request: Request,
) -> RunJobResponse:
    if not user.can_trigger_sync:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"error": "forbidden", "message": "User role does not allow triggering jobs"},
        )

    client = _get_databricks_client(raw_request)

    try:
        run_id = client.run_job(job_id, body.parameters or None)
    except Exception as exc:
        logger.error("Failed to trigger Databricks job %d: %s", job_id, exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={"error": "databricks_error", "message": str(exc)},
        )

    logger.info(
        "User %s triggered Databricks job %d → run_id=%d (params=%s)",
        user.email, job_id, run_id, body.parameters,
    )

    return RunJobResponse(job_id=job_id, run_id=run_id)


@router.get(
    "/runs/{run_id}",
    response_model=RunStatusResponse,
    summary="Get Databricks run status",
    description="Get status, progress, and task details for a Databricks job run.",
)
async def get_run_status(
    run_id: int,
    user: CurrentAzureADUser,
    raw_request: Request,
) -> RunStatusResponse:
    client = _get_databricks_client(raw_request)

    try:
        run = client.get_run(run_id)
    except Exception as exc:
        logger.error("Failed to get Databricks run %d: %s", run_id, exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={"error": "databricks_error", "message": str(exc)},
        )

    lifecycle = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else "UNKNOWN"
    result_state = run.state.result_state.value if run.state and run.state.result_state else None
    state_message = run.state.state_message if run.state else None

    tasks: list[TaskInfo] = []
    for task in run.tasks or []:
        task_lifecycle = task.state.life_cycle_state.value if task.state and task.state.life_cycle_state else None
        task_result = task.state.result_state.value if task.state and task.state.result_state else None
        tasks.append(
            TaskInfo(
                task_key=task.task_key,
                run_id=task.run_id,
                state=task_lifecycle,
                result_state=task_result,
                start_time=_epoch_ms_to_datetime(task.start_time),
                end_time=_epoch_ms_to_datetime(task.end_time),
            )
        )

    return RunStatusResponse(
        run_id=run.run_id,
        job_id=run.job_id,
        run_name=run.run_name,
        state=lifecycle,
        result_state=result_state,
        state_message=state_message,
        start_time=_epoch_ms_to_datetime(run.start_time),
        end_time=_epoch_ms_to_datetime(run.end_time),
        run_duration_ms=run.run_duration,
        run_page_url=run.run_page_url,
        tasks=tasks,
    )
