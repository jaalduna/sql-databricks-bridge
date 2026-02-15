"""Calibration pipeline step and substep models.

Defines the structure for tracking multi-step calibration pipeline progress:
  1. sync        – SQL Server → Databricks (000-sql-databricks-bridge.{country})
  2. copy_bronze – Copy to 001-calibration-3-0.bronze-{country}
  3. merge       – Merge into silver layer
  4. simulate_weights – Simulate weights if not available
  5. simulate_kpis    – Simulate KPIs (all / bc / original)
  6. calculate_targets – Calculate penetration & volume targets
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class StepStatus(str, Enum):
    """Status of a pipeline step or substep."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class SubStep(BaseModel):
    """A substep within a pipeline step (e.g. a single query or table)."""

    name: str
    status: StepStatus = StepStatus.PENDING
    started_at: datetime | None = None
    completed_at: datetime | None = None
    rows_affected: int | None = None
    error: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class PipelineStep(BaseModel):
    """A top-level step in the calibration pipeline."""

    step_id: str = Field(..., description="Unique identifier: sync, copy_bronze, merge, etc.")
    name: str = Field(..., description="Human-readable name")
    order: int = Field(..., description="Execution order (1-based)")
    status: StepStatus = StepStatus.PENDING
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_seconds: float | None = None
    error: str | None = None
    substeps: list[SubStep] = Field(default_factory=list)
    databricks_run_id: int | None = Field(
        default=None, description="Databricks job run ID (if tracked via Databricks Jobs API)"
    )

    @property
    def substeps_total(self) -> int:
        return len(self.substeps)

    @property
    def substeps_completed(self) -> int:
        return sum(1 for s in self.substeps if s.status == StepStatus.COMPLETED)

    @property
    def substeps_failed(self) -> int:
        return sum(1 for s in self.substeps if s.status == StepStatus.FAILED)

    @property
    def progress_pct(self) -> float:
        if not self.substeps:
            if self.status == StepStatus.COMPLETED:
                return 100.0
            if self.status == StepStatus.RUNNING:
                return 50.0
            return 0.0
        return round(self.substeps_completed / self.substeps_total * 100, 1)


class CalibrationPipeline(BaseModel):
    """Full calibration pipeline with all steps for a country/period."""

    pipeline_id: str = Field(..., description="Unique pipeline run ID")
    country: str
    period: str | None = None
    status: StepStatus = StepStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    triggered_by: str = ""
    steps: list[PipelineStep] = Field(default_factory=list)
    error: str | None = None

    @property
    def current_step(self) -> PipelineStep | None:
        for step in self.steps:
            if step.status == StepStatus.RUNNING:
                return step
        return None

    @property
    def steps_completed(self) -> int:
        return sum(1 for s in self.steps if s.status == StepStatus.COMPLETED)

    @property
    def progress_pct(self) -> float:
        if not self.steps:
            return 0.0
        return round(self.steps_completed / len(self.steps) * 100, 1)


# --- Factory for the standard calibration pipeline steps ---

CALIBRATION_STEPS = [
    {
        "step_id": "sync",
        "name": "Sync SQL Server → Databricks",
        "order": 1,
    },
    {
        "step_id": "copy_bronze",
        "name": "Copy to Bronze Layer",
        "order": 2,
    },
    {
        "step_id": "merge",
        "name": "Merge to Silver Layer",
        "order": 3,
    },
    {
        "step_id": "simulate_weights",
        "name": "Simulate Weights",
        "order": 4,
    },
    {
        "step_id": "simulate_kpis",
        "name": "Simulate KPIs",
        "order": 5,
    },
    {
        "step_id": "calculate_targets",
        "name": "Calculate Penetration & Volume Targets",
        "order": 6,
    },
]


def build_calibration_steps() -> list[PipelineStep]:
    """Build the standard set of calibration pipeline steps."""
    return [PipelineStep(**cfg) for cfg in CALIBRATION_STEPS]


# --- Standard substeps per step ---

SYNC_SUBSTEP_QUERIES = {
    "bolivia": [
        "j_atoscompra_new",
        "hato_cabecalho",
        "cl_catproduto_new",
        "cl_painelista",
        "cl_cidades",
    ],
}

KPI_SUBSTEPS = [
    {"name": "kpi_all", "metadata": {"variant": "all"}},
    {"name": "kpi_bc", "metadata": {"variant": "bc"}},
    {"name": "kpi_original", "metadata": {"variant": "original"}},
]

TARGET_SUBSTEPS = [
    {"name": "penetration_targets"},
    {"name": "volume_targets"},
]
