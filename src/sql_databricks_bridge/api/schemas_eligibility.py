"""Pydantic schemas for the Elegibilidad API."""

from typing import Any

from pydantic import BaseModel, Field


class EligibilityRunCreate(BaseModel):
    country: str = Field(..., description="Country code (e.g. 'CO', 'CL')")
    period: int = Field(..., description="Period in YYYYMM format (e.g. 202401)")
    parameters: dict[str, Any] | None = Field(
        default=None, description="Snapshot of eligibility parameters used for this run"
    )


class EligibilityStepResponse(BaseModel):
    name: str
    label: str
    status: str
    started_at: str | None = None
    completed_at: str | None = None


class EligibilityFileResponse(BaseModel):
    file_id: str
    run_id: str
    stage: int
    direction: str
    filename: str
    stored_path: str
    file_size_bytes: int | None = None
    uploaded_by: str | None = None
    created_at: str | None = None


class EligibilityRunResponse(BaseModel):
    run_id: str
    country: str
    period: int
    status: str
    parameters_json: dict[str, Any] | None = None
    phase1_metrics_json: dict[str, Any] | None = None
    created_by: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    approved_by: str | None = None
    approved_at: str | None = None
    approval_comment: str | None = None
    error_message: str | None = None
    steps: list[EligibilityStepResponse] = Field(default_factory=list)
    files: list[EligibilityFileResponse] = Field(default_factory=list)
    current_step: str | None = None
    started_at: str | None = None
    completed_at: str | None = None


class EligibilityRunUpdate(BaseModel):
    status: str | None = None
    approved_by: str | None = None
    approval_comment: str | None = None
    error_message: str | None = None
    phase1_metrics_json: dict[str, Any] | None = None


class EligibilityRunList(BaseModel):
    items: list[EligibilityRunResponse]
    total: int
