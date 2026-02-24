"""Elegibilidad run management API endpoints."""

import csv
import io
import json
import logging
import os
import random
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request, UploadFile, File, status
from fastapi.responses import StreamingResponse

from sql_databricks_bridge.api.dependencies import CurrentAzureADUser
from sql_databricks_bridge.api.schemas_eligibility import (
    EligibilityFileResponse,
    EligibilityRunCreate,
    EligibilityRunList,
    EligibilityRunResponse,
    EligibilityRunUpdate,
    EligibilityStepResponse,
)
from sql_databricks_bridge.db.eligibility_store import (
    delete_run,
    get_file,
    get_files_for_run,
    get_run,
    init_eligibility_db,
    insert_file,
    insert_run,
    list_runs,
    update_run,
)
from sql_databricks_bridge.models.eligibility import (
    CANCELLABLE_STATUSES,
    build_eligibility_steps,
    update_step_status,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/eligibility", tags=["Elegibilidad"])


def _get_db_path(request: Request) -> str:
    """Resolve the eligibility SQLite DB path from app state or default."""
    sqlite_db_path = getattr(request.app.state, "sqlite_db_path", None)
    if sqlite_db_path:
        db_path = os.path.join(os.path.dirname(sqlite_db_path), "eligibility.db")
    else:
        db_path = "eligibility.db"
    init_eligibility_db(db_path)
    return db_path


def _get_data_dir(request: Request) -> str:
    """Resolve the base data directory for file storage."""
    sqlite_db_path = getattr(request.app.state, "sqlite_db_path", None)
    if sqlite_db_path:
        return os.path.join(os.path.dirname(sqlite_db_path), ".bridge_data")
    return ".bridge_data"


def _run_to_response(run: dict, db_path: str) -> EligibilityRunResponse:
    steps_data = run.get("steps_json") or []
    files_data = get_files_for_run(db_path, run["run_id"])

    steps = [EligibilityStepResponse(**s) for s in steps_data]
    files = [EligibilityFileResponse(**f) for f in files_data]

    return EligibilityRunResponse(
        run_id=run["run_id"],
        country=run["country"],
        period=run["period"],
        status=run["status"],
        parameters_json=run.get("parameters_json"),
        phase1_metrics_json=run.get("phase1_metrics_json"),
        created_by=run.get("created_by"),
        created_at=run.get("created_at"),
        updated_at=run.get("updated_at"),
        approved_by=run.get("approved_by"),
        approved_at=run.get("approved_at"),
        approval_comment=run.get("approval_comment"),
        error_message=run.get("error_message"),
        steps=steps,
        files=files,
        current_step=run.get("current_step"),
        started_at=run.get("started_at"),
        completed_at=run.get("completed_at"),
    )


def _generate_mock_csv(country: str, period: int) -> str:
    """Generate a mock eligibility CSV with sample data."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["panelistID", "period", "status", "score", "eligible"])
    for i in range(1, 13):
        score = round(random.uniform(0.1, 1.0), 3)
        eligible = 1 if score >= 0.5 else 0
        writer.writerow([f"PAN{country}{i:04d}", period, "evaluated", score, eligible])
    return output.getvalue()


def _run_stage_job(db_path: str, data_dir: str, run_id: str, stage: int) -> None:
    """Background job that simulates a stage execution."""
    import time
    time.sleep(2)

    run = get_run(db_path, run_id)
    if not run:
        return

    # Check run hasn't been cancelled while we were sleeping
    if run["status"] in ("cancelled", "failed"):
        return

    now = datetime.now(timezone.utc).isoformat()
    country = run["country"]
    period = run["period"]

    # Generate mock CSV
    csv_content = _generate_mock_csv(country, period)
    stage_dir = os.path.join(
        data_dir, "eligibility", run_id, f"stage{stage}", "download"
    )
    os.makedirs(stage_dir, exist_ok=True)
    filename = f"elegibilidad_{country}_{period}_fase{stage}.csv"
    file_path = os.path.join(stage_dir, filename)

    with open(file_path, "w", newline="") as f:
        f.write(csv_content)

    file_size = os.path.getsize(file_path)

    # Record file in DB
    file_id = str(uuid.uuid4())
    insert_file(
        db_path,
        file_id=file_id,
        run_id=run_id,
        stage=stage,
        direction="download",
        filename=filename,
        stored_path=file_path,
        file_size_bytes=file_size,
        created_at=now,
    )

    # Update steps
    steps = run.get("steps_json") or []
    if isinstance(steps, str):
        steps = json.loads(steps)

    job_step = f"stage{stage}_job"
    download_step = f"stage{stage}_download"
    steps = update_step_status(steps, job_step, "completed")
    steps = update_step_status(steps, download_step, "completed")

    ready_status = f"stage{stage}_ready"
    current_step = f"stage{stage}_upload"

    update_run(
        db_path,
        run_id,
        status=ready_status,
        steps_json=json.dumps(steps),
        current_step=current_step,
        updated_at=now,
    )
    logger.info(f"Stage {stage} job completed for run {run_id}, status → {ready_status}")


@router.post(
    "/runs",
    response_model=EligibilityRunResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create eligibility run",
    description="Create a new eligibility run for a country/period.",
)
async def create_run(
    body: EligibilityRunCreate,
    request: Request,
    user: CurrentAzureADUser,
) -> EligibilityRunResponse:
    db_path = _get_db_path(request)
    now = datetime.now(timezone.utc).isoformat()
    run_id = str(uuid.uuid4())

    steps = build_eligibility_steps()

    insert_run(
        db_path,
        run_id=run_id,
        country=body.country,
        period=body.period,
        status="pending",
        parameters_json=body.parameters,
        created_by=user.email,
        created_at=now,
        updated_at=now,
    )
    # Set steps_json and current_step after insert (these are new columns)
    update_run(
        db_path,
        run_id,
        steps_json=json.dumps(steps),
        current_step="stage1_job",
    )
    logger.info(f"Created eligibility run {run_id} for {body.country}/{body.period}")

    run = get_run(db_path, run_id)
    assert run is not None
    return _run_to_response(run, db_path)


@router.get(
    "/runs",
    response_model=EligibilityRunList,
    summary="List eligibility runs",
    description="List runs with optional filters.",
)
async def list_eligibility_runs(
    request: Request,
    user: CurrentAzureADUser,
    country: str | None = Query(default=None),
    period: int | None = Query(default=None),
    run_status: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> EligibilityRunList:
    db_path = _get_db_path(request)
    items, total = list_runs(
        db_path,
        country=country,
        period=period,
        status=run_status,
        limit=limit,
        offset=offset,
    )
    return EligibilityRunList(
        items=[_run_to_response(r, db_path) for r in items],
        total=total,
    )


@router.get(
    "/runs/{run_id}",
    response_model=EligibilityRunResponse,
    summary="Get eligibility run",
    description="Get a single run by ID.",
)
async def get_eligibility_run(
    run_id: str,
    request: Request,
    user: CurrentAzureADUser,
) -> EligibilityRunResponse:
    db_path = _get_db_path(request)
    run = get_run(db_path, run_id)
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Eligibility run not found: {run_id}"},
        )
    return _run_to_response(run, db_path)


@router.patch(
    "/runs/{run_id}",
    response_model=EligibilityRunResponse,
    summary="Update eligibility run",
    description="Update run status, approval, or error.",
)
async def update_eligibility_run(
    run_id: str,
    body: EligibilityRunUpdate,
    request: Request,
    user: CurrentAzureADUser,
) -> EligibilityRunResponse:
    db_path = _get_db_path(request)
    run = get_run(db_path, run_id)
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Eligibility run not found: {run_id}"},
        )

    now = datetime.now(timezone.utc).isoformat()
    fields: dict = {"updated_at": now}

    if body.status is not None:
        fields["status"] = body.status
    if body.approved_by is not None:
        fields["approved_by"] = body.approved_by
        fields["approved_at"] = now
    if body.approval_comment is not None:
        fields["approval_comment"] = body.approval_comment
    if body.error_message is not None:
        fields["error_message"] = body.error_message
    if body.phase1_metrics_json is not None:
        fields["phase1_metrics_json"] = json.dumps(body.phase1_metrics_json)

    update_run(db_path, run_id, **fields)
    updated = get_run(db_path, run_id)
    assert updated is not None
    return _run_to_response(updated, db_path)


@router.delete(
    "/runs/{run_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete eligibility run",
    description="Delete a run by ID.",
)
async def delete_eligibility_run(
    run_id: str,
    request: Request,
    user: CurrentAzureADUser,
) -> None:
    db_path = _get_db_path(request)
    if not delete_run(db_path, run_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Eligibility run not found: {run_id}"},
        )


@router.post(
    "/runs/{run_id}/execute",
    response_model=EligibilityRunResponse,
    summary="Execute stage 1",
    description="Start stage 1 eligibility processing. Returns immediately with status=stage1_running.",
)
async def execute_stage1(
    run_id: str,
    request: Request,
    user: CurrentAzureADUser,
    background_tasks: BackgroundTasks,
) -> EligibilityRunResponse:
    db_path = _get_db_path(request)
    data_dir = _get_data_dir(request)
    run = get_run(db_path, run_id)
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Eligibility run not found: {run_id}"},
        )

    if run["status"] != "pending":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "invalid_status",
                "message": f"Cannot execute: run status is '{run['status']}', expected 'pending'",
            },
        )

    now = datetime.now(timezone.utc).isoformat()
    steps = run.get("steps_json") or []
    if isinstance(steps, str):
        steps = json.loads(steps)
    steps = update_step_status(steps, "stage1_job", "running")

    update_run(
        db_path,
        run_id,
        status="stage1_running",
        steps_json=json.dumps(steps),
        current_step="stage1_job",
        started_at=now,
        updated_at=now,
    )

    background_tasks.add_task(_run_stage_job, db_path, data_dir, run_id, 1)

    updated = get_run(db_path, run_id)
    assert updated is not None
    return _run_to_response(updated, db_path)


@router.post(
    "/runs/{run_id}/execute-stage2",
    response_model=EligibilityRunResponse,
    summary="Execute stage 2",
    description="Start stage 2 eligibility processing. Requires status=stage1_uploaded.",
)
async def execute_stage2(
    run_id: str,
    request: Request,
    user: CurrentAzureADUser,
    background_tasks: BackgroundTasks,
) -> EligibilityRunResponse:
    db_path = _get_db_path(request)
    data_dir = _get_data_dir(request)
    run = get_run(db_path, run_id)
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Eligibility run not found: {run_id}"},
        )

    if run["status"] != "stage1_uploaded":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "invalid_status",
                "message": f"Cannot execute stage 2: run status is '{run['status']}', expected 'stage1_uploaded'",
            },
        )

    now = datetime.now(timezone.utc).isoformat()
    steps = run.get("steps_json") or []
    if isinstance(steps, str):
        steps = json.loads(steps)
    steps = update_step_status(steps, "stage2_job", "running")

    update_run(
        db_path,
        run_id,
        status="stage2_running",
        steps_json=json.dumps(steps),
        current_step="stage2_job",
        updated_at=now,
    )

    background_tasks.add_task(_run_stage_job, db_path, data_dir, run_id, 2)

    updated = get_run(db_path, run_id)
    assert updated is not None
    return _run_to_response(updated, db_path)


@router.get(
    "/runs/{run_id}/files",
    response_model=list[EligibilityFileResponse],
    summary="List files for a run",
)
async def list_run_files(
    run_id: str,
    request: Request,
    user: CurrentAzureADUser,
) -> list[EligibilityFileResponse]:
    db_path = _get_db_path(request)
    run = get_run(db_path, run_id)
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Eligibility run not found: {run_id}"},
        )
    files = get_files_for_run(db_path, run_id)
    return [EligibilityFileResponse(**f) for f in files]


@router.get(
    "/runs/{run_id}/files/{file_id}/download",
    summary="Download a file",
)
async def download_file(
    run_id: str,
    file_id: str,
    request: Request,
    user: CurrentAzureADUser,
):
    db_path = _get_db_path(request)
    run = get_run(db_path, run_id)
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Eligibility run not found: {run_id}"},
        )

    file_record = get_file(db_path, file_id)
    if not file_record or file_record["run_id"] != run_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"File not found: {file_id}"},
        )

    stored_path = file_record["stored_path"]
    if not os.path.exists(stored_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "file_missing", "message": "File not found on disk"},
        )

    def iter_file():
        with open(stored_path, "rb") as f:
            yield from f

    return StreamingResponse(
        iter_file(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{file_record["filename"]}"'},
    )


@router.post(
    "/runs/{run_id}/upload",
    response_model=EligibilityRunResponse,
    summary="Upload a CSV file",
    description="Upload a CSV for the current stage. Detects stage from run status.",
)
async def upload_file(
    run_id: str,
    request: Request,
    user: CurrentAzureADUser,
    file: UploadFile = File(...),
) -> EligibilityRunResponse:
    db_path = _get_db_path(request)
    data_dir = _get_data_dir(request)
    run = get_run(db_path, run_id)
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Eligibility run not found: {run_id}"},
        )

    # Detect stage from status
    status_to_stage = {
        "stage1_ready": 1,
        "stage2_ready": 2,
    }
    current_status = run["status"]
    stage = status_to_stage.get(current_status)
    if stage is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "invalid_status",
                "message": f"Cannot upload: run status is '{current_status}', expected 'stage1_ready' or 'stage2_ready'",
            },
        )

    now = datetime.now(timezone.utc).isoformat()

    # Save file to disk
    upload_dir = os.path.join(
        data_dir, "eligibility", run_id, f"stage{stage}", "upload"
    )
    os.makedirs(upload_dir, exist_ok=True)
    filename = file.filename or f"upload_{now}.csv"
    file_path = os.path.join(upload_dir, filename)

    content = await file.read()
    with open(file_path, "wb") as f:
        f.write(content)

    file_size = len(content)

    # Record file in DB
    file_id = str(uuid.uuid4())
    insert_file(
        db_path,
        file_id=file_id,
        run_id=run_id,
        stage=stage,
        direction="upload",
        filename=filename,
        stored_path=file_path,
        file_size_bytes=file_size,
        uploaded_by=user.email,
        created_at=now,
    )

    # Update steps and status
    steps = run.get("steps_json") or []
    if isinstance(steps, str):
        steps = json.loads(steps)
    upload_step = f"stage{stage}_upload"
    steps = update_step_status(steps, upload_step, "completed")

    new_status = f"stage{stage}_uploaded"
    next_step = "stage2_job" if stage == 1 else "finalize"

    update_run(
        db_path,
        run_id,
        status=new_status,
        steps_json=json.dumps(steps),
        current_step=next_step,
        updated_at=now,
    )

    updated = get_run(db_path, run_id)
    assert updated is not None
    return _run_to_response(updated, db_path)


@router.post(
    "/runs/{run_id}/finalize",
    response_model=EligibilityRunResponse,
    summary="Finalize run",
    description="Mark run as finalized. Requires status=stage2_uploaded.",
)
async def finalize_run(
    run_id: str,
    request: Request,
    user: CurrentAzureADUser,
) -> EligibilityRunResponse:
    db_path = _get_db_path(request)
    run = get_run(db_path, run_id)
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Eligibility run not found: {run_id}"},
        )

    if run["status"] != "stage2_uploaded":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "invalid_status",
                "message": f"Cannot finalize: run status is '{run['status']}', expected 'stage2_uploaded'",
            },
        )

    now = datetime.now(timezone.utc).isoformat()
    steps = run.get("steps_json") or []
    if isinstance(steps, str):
        steps = json.loads(steps)
    steps = update_step_status(steps, "finalize", "completed")
    steps = update_step_status(steps, "complete", "completed")

    update_run(
        db_path,
        run_id,
        status="finalized",
        steps_json=json.dumps(steps),
        current_step="complete",
        completed_at=now,
        updated_at=now,
    )

    updated = get_run(db_path, run_id)
    assert updated is not None
    return _run_to_response(updated, db_path)


@router.post(
    "/runs/{run_id}/cancel",
    response_model=EligibilityRunResponse,
    summary="Cancel run",
    description="Cancel a run from any active state.",
)
async def cancel_run(
    run_id: str,
    request: Request,
    user: CurrentAzureADUser,
) -> EligibilityRunResponse:
    db_path = _get_db_path(request)
    run = get_run(db_path, run_id)
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Eligibility run not found: {run_id}"},
        )

    if run["status"] not in CANCELLABLE_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "invalid_status",
                "message": f"Cannot cancel: run status is '{run['status']}'",
            },
        )

    now = datetime.now(timezone.utc).isoformat()
    update_run(
        db_path,
        run_id,
        status="cancelled",
        updated_at=now,
        completed_at=now,
    )

    updated = get_run(db_path, run_id)
    assert updated is not None
    return _run_to_response(updated, db_path)
