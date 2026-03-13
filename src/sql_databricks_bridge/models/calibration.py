"""Calibration step models matching the calibration-frontend API contract.

Step names and structure must match exactly what the frontend expects
(see calibration_frontend/src/types/api.ts CalibrationStep interface).
"""
from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

# The exact step names expected by the calibration frontend
CalibrationStepName = Literal[
    "sync_data",
    "copy_to_calibration",
    "merge_data",
    "simulate_kpis",
    "calculate_penetration",
    "download_csv",
]

CALIBRATION_STEP_ORDER: list[CalibrationStepName] = [
    "sync_data",
    "copy_to_calibration",
    "merge_data",
    "simulate_kpis",
    "calculate_penetration",
    "download_csv",
]


class DatabricksTaskStatus(BaseModel):
    """Status of a single Databricks task within a multi-task job run."""

    task_key: str
    status: str = "pending"  # pending|running|completed|failed|skipped
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None


class CalibrationStep(BaseModel):
    """A single calibration pipeline step. Serializes to the exact shape
    expected by the calibration frontend."""

    name: CalibrationStepName
    status: str = "pending"  # JobStatus: pending|running|completed|failed|cancelled
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None
    databricks_run_id: int | None = Field(default=None, exclude=True)
    tasks: list[DatabricksTaskStatus] = Field(default_factory=list)
    """Per-task progress from Databricks multi-task jobs."""


class CalibrationJobInfo(BaseModel):
    """Calibration step tracking attached to a trigger job."""

    job_id: str
    country: str = ""
    period: str | None = None
    aggregations: dict[str, bool] = Field(default_factory=dict)
    steps: list[CalibrationStep] = Field(default_factory=list)

    @property
    def current_step(self) -> CalibrationStepName | None:
        """Return the name of the currently running step, or None."""
        for step in self.steps:
            if step.status == "running":
                return step.name
        return None

    def get_step(self, name: CalibrationStepName) -> CalibrationStep | None:
        for step in self.steps:
            if step.name == name:
                return step
        return None


def build_calibration_steps() -> list[CalibrationStep]:
    """Build all 6 calibration steps in pending state."""
    return [CalibrationStep(name=name) for name in CALIBRATION_STEP_ORDER]
