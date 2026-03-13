"""Elegibilidad run management API endpoints.

Workflow:
  1. Create run with parameters → pending
  2. Execute pipeline → running → results_ready  (iterative: user can re-run)
  3. Approve results → applying_sql (background writes to SQL)
  4. SQL applied → mordom_downloading → mordom_ready (MorDom CSV available)
  5. User downloads MorDom, reviews, uploads corrected CSV → mordom_uploaded
  6. Apply MorDom corrections → applying_mordom → ready
"""

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
        # The DB is already inside .bridge_data/, so use its directory directly
        return os.path.dirname(sqlite_db_path)
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


def _generate_mock_results_csv(country: str, period: int) -> str:
    """Generate mock eligibility results CSV."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["panelistID", "period", "status", "score", "eligible",
                     "purchases", "categories", "mortality_flag"])
    for i in range(1, 25):
        score = round(random.uniform(0.1, 1.0), 3)
        eligible = 1 if score >= 0.5 else 0
        purchases = random.randint(1, 20)
        cats = random.randint(1, 8)
        mortality = 0 if random.random() > 0.15 else 1
        writer.writerow([f"PAN{country}{i:04d}", period, "evaluated",
                         score, eligible, purchases, cats, mortality])
    return output.getvalue()


def _generate_mock_mordom_csv(country: str, period: int) -> str:
    """Generate mock MorDom CSV with new households."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["panelistID", "period", "household_size", "income_bracket",
                     "region", "scanner_flag", "entry_date", "attributes_complete",
                     "action"])
    for i in range(1, 15):
        hh_size = random.randint(1, 6)
        income = random.choice(["A", "B", "C", "D", "E"])
        region = random.randint(1, 13)
        scanner = random.choice([0, 1])
        complete = 1 if random.random() > 0.25 else 0
        writer.writerow([f"PAN{country}{i:04d}", period, hh_size, income,
                         region, scanner, "2024-01-15", complete, "keep"])
    return output.getvalue()


# ── Background jobs ─────────────────────────────────────────────────────────


def _run_pipeline_job(db_path: str, data_dir: str, run_id: str) -> None:
    """Background job: simulate pipeline execution (Phase 0–9)."""
    import time
    time.sleep(3)

    run = get_run(db_path, run_id)
    if not run or run["status"] in ("cancelled", "failed"):
        return

    now = datetime.now(timezone.utc).isoformat()
    country = run["country"]
    period = run["period"]

    # Generate results CSV
    csv_content = _generate_mock_results_csv(country, period)
    results_dir = os.path.join(data_dir, "eligibility", run_id, "results")
    os.makedirs(results_dir, exist_ok=True)
    filename = f"elegibilidad_{country}_{period}_resultados.csv"
    file_path = os.path.join(results_dir, filename)

    with open(file_path, "w", newline="") as f:
        f.write(csv_content)

    file_size = os.path.getsize(file_path)
    file_id = str(uuid.uuid4())
    insert_file(
        db_path,
        file_id=file_id,
        run_id=run_id,
        stage=1,
        direction="download",
        filename=filename,
        stored_path=file_path,
        file_size_bytes=file_size,
        created_at=now,
    )

    steps = run.get("steps_json") or []
    if isinstance(steps, str):
        steps = json.loads(steps)

    steps = update_step_status(steps, "run_pipeline", "completed")
    steps = update_step_status(steps, "download_results", "completed")

    update_run(
        db_path,
        run_id,
        status="results_ready",
        steps_json=json.dumps(steps),
        current_step="approve",
        updated_at=now,
    )
    logger.info(f"Pipeline completed for run {run_id}, status → results_ready")


def _run_apply_sql_job(db_path: str, data_dir: str, run_id: str) -> None:
    """Background job: write results to SQL Server, then download MorDom."""
    import time
    time.sleep(3)

    run = get_run(db_path, run_id)
    if not run or run["status"] in ("cancelled", "failed"):
        return

    now = datetime.now(timezone.utc).isoformat()
    country = run["country"]
    period = run["period"]

    steps = run.get("steps_json") or []
    if isinstance(steps, str):
        steps = json.loads(steps)

    # Mark SQL apply completed
    steps = update_step_status(steps, "apply_sql", "completed")

    update_run(
        db_path,
        run_id,
        status="mordom_downloading",
        steps_json=json.dumps(steps),
        current_step="download_mordom",
        updated_at=now,
    )
    logger.info(f"SQL applied for run {run_id}, downloading MorDom...")

    # Simulate MorDom download
    time.sleep(2)

    # Generate MorDom CSV
    csv_content = _generate_mock_mordom_csv(country, period)
    mordom_dir = os.path.join(data_dir, "eligibility", run_id, "mordom")
    os.makedirs(mordom_dir, exist_ok=True)
    filename = f"mordom_{country}_{period}_hogares.csv"
    file_path = os.path.join(mordom_dir, filename)

    with open(file_path, "w", newline="") as f:
        f.write(csv_content)

    file_size = os.path.getsize(file_path)
    file_id = str(uuid.uuid4())
    insert_file(
        db_path,
        file_id=file_id,
        run_id=run_id,
        stage=2,
        direction="download",
        filename=filename,
        stored_path=file_path,
        file_size_bytes=file_size,
        created_at=now,
    )

    steps = update_step_status(steps, "download_mordom", "completed")

    update_run(
        db_path,
        run_id,
        status="mordom_ready",
        steps_json=json.dumps(steps),
        current_step="upload_mordom",
        updated_at=now,
    )
    logger.info(f"MorDom ready for run {run_id}, status → mordom_ready")


def _run_apply_mordom_job(db_path: str, data_dir: str, run_id: str) -> None:
    """Background job: apply MorDom corrections and mark as ready."""
    import time
    time.sleep(2)

    run = get_run(db_path, run_id)
    if not run or run["status"] in ("cancelled", "failed"):
        return

    now = datetime.now(timezone.utc).isoformat()
    steps = run.get("steps_json") or []
    if isinstance(steps, str):
        steps = json.loads(steps)

    steps = update_step_status(steps, "apply_mordom", "completed")
    steps = update_step_status(steps, "complete", "completed")

    update_run(
        db_path,
        run_id,
        status="ready",
        steps_json=json.dumps(steps),
        current_step="complete",
        completed_at=now,
        updated_at=now,
    )
    logger.info(f"MorDom applied for run {run_id}, status → ready")


# ── CRUD endpoints ──────────────────────────────────────────────────────────


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
    update_run(
        db_path,
        run_id,
        steps_json=json.dumps(steps),
        current_step="run_pipeline",
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


# ── Pipeline execution ──────────────────────────────────────────────────────


@router.post(
    "/runs/{run_id}/execute",
    response_model=EligibilityRunResponse,
    summary="Execute pipeline",
    description="Run the eligibility pipeline (Phase 0–9). Returns immediately with status=running.",
)
async def execute_pipeline(
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
    steps = update_step_status(steps, "run_pipeline", "running")

    update_run(
        db_path,
        run_id,
        status="running",
        steps_json=json.dumps(steps),
        current_step="run_pipeline",
        started_at=now,
        updated_at=now,
    )

    background_tasks.add_task(_run_pipeline_job, db_path, data_dir, run_id)

    updated = get_run(db_path, run_id)
    assert updated is not None
    return _run_to_response(updated, db_path)


# ── Approve & Apply to SQL ──────────────────────────────────────────────────


@router.post(
    "/runs/{run_id}/approve",
    response_model=EligibilityRunResponse,
    summary="Approve results and apply to SQL",
    description="Approve the eligibility results. Triggers writing to SQL Server and MorDom download.",
)
async def approve_and_apply(
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

    if run["status"] != "results_ready":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "invalid_status",
                "message": f"Cannot approve: run status is '{run['status']}', expected 'results_ready'",
            },
        )

    now = datetime.now(timezone.utc).isoformat()
    steps = run.get("steps_json") or []
    if isinstance(steps, str):
        steps = json.loads(steps)
    steps = update_step_status(steps, "approve", "completed")
    steps = update_step_status(steps, "apply_sql", "running")

    update_run(
        db_path,
        run_id,
        status="applying_sql",
        steps_json=json.dumps(steps),
        current_step="apply_sql",
        approved_by=user.email,
        approved_at=now,
        updated_at=now,
    )

    background_tasks.add_task(_run_apply_sql_job, db_path, data_dir, run_id)

    updated = get_run(db_path, run_id)
    assert updated is not None
    return _run_to_response(updated, db_path)


# ── MorDom upload & apply ───────────────────────────────────────────────────


@router.post(
    "/runs/{run_id}/upload",
    response_model=EligibilityRunResponse,
    summary="Upload corrected MorDom CSV",
    description="Upload corrected MorDom CSV with households to remove/keep.",
)
async def upload_mordom(
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

    if run["status"] != "mordom_ready":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "invalid_status",
                "message": f"Cannot upload: run status is '{run['status']}', expected 'mordom_ready'",
            },
        )

    now = datetime.now(timezone.utc).isoformat()

    # Save file to disk
    upload_dir = os.path.join(data_dir, "eligibility", run_id, "mordom", "upload")
    os.makedirs(upload_dir, exist_ok=True)
    filename = file.filename or f"mordom_corrected_{now}.csv"
    file_path = os.path.join(upload_dir, filename)

    content = await file.read()
    with open(file_path, "wb") as f:
        f.write(content)

    file_size = len(content)
    file_id = str(uuid.uuid4())
    insert_file(
        db_path,
        file_id=file_id,
        run_id=run_id,
        stage=2,
        direction="upload",
        filename=filename,
        stored_path=file_path,
        file_size_bytes=file_size,
        uploaded_by=user.email,
        created_at=now,
    )

    steps = run.get("steps_json") or []
    if isinstance(steps, str):
        steps = json.loads(steps)
    steps = update_step_status(steps, "upload_mordom", "completed")

    update_run(
        db_path,
        run_id,
        status="mordom_uploaded",
        steps_json=json.dumps(steps),
        current_step="apply_mordom",
        updated_at=now,
    )

    updated = get_run(db_path, run_id)
    assert updated is not None
    return _run_to_response(updated, db_path)


@router.post(
    "/runs/{run_id}/apply-mordom",
    response_model=EligibilityRunResponse,
    summary="Apply MorDom corrections",
    description="Apply final MorDom corrections and mark eligibility as ready.",
)
async def apply_mordom(
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

    if run["status"] != "mordom_uploaded":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "invalid_status",
                "message": f"Cannot apply MorDom: run status is '{run['status']}', expected 'mordom_uploaded'",
            },
        )

    now = datetime.now(timezone.utc).isoformat()
    steps = run.get("steps_json") or []
    if isinstance(steps, str):
        steps = json.loads(steps)
    steps = update_step_status(steps, "apply_mordom", "running")

    update_run(
        db_path,
        run_id,
        status="applying_mordom",
        steps_json=json.dumps(steps),
        current_step="apply_mordom",
        updated_at=now,
    )

    background_tasks.add_task(_run_apply_mordom_job, db_path, data_dir, run_id)

    updated = get_run(db_path, run_id)
    assert updated is not None
    return _run_to_response(updated, db_path)


# ── File management ─────────────────────────────────────────────────────────


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


# ── Cancel ──────────────────────────────────────────────────────────────────


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
