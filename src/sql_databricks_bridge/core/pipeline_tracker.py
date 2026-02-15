"""Calibration pipeline tracker.

Manages in-memory state for calibration pipeline runs and optionally
polls Databricks job runs for status updates.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from uuid import uuid4

from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.models.pipeline import (
    CALIBRATION_STEPS,
    KPI_SUBSTEPS,
    TARGET_SUBSTEPS,
    CalibrationPipeline,
    PipelineStep,
    StepStatus,
    SubStep,
    build_calibration_steps,
)

logger = logging.getLogger(__name__)


class PipelineTracker:
    """Tracks calibration pipeline runs in memory.

    Each pipeline run goes through 6 ordered steps.  The tracker allows the
    API layer (or a background poller) to advance steps and substeps.

    It can also poll Databricks Jobs API for run status when a step has a
    ``databricks_run_id`` attached.
    """

    def __init__(self) -> None:
        self._pipelines: dict[str, CalibrationPipeline] = {}

    # --- CRUD ---

    def create_pipeline(
        self,
        country: str,
        period: str | None = None,
        triggered_by: str = "",
        sync_queries: list[str] | None = None,
    ) -> CalibrationPipeline:
        """Create a new calibration pipeline run with all standard steps."""
        pipeline_id = str(uuid4())
        steps = build_calibration_steps()

        # Populate sync substeps from query list
        if sync_queries:
            sync_step = steps[0]  # order=1 → sync
            sync_step.substeps = [SubStep(name=q) for q in sync_queries]

        # Populate KPI substeps
        kpi_step = steps[4]  # order=5 → simulate_kpis
        kpi_step.substeps = [SubStep(name=s["name"], metadata=s.get("metadata", {})) for s in KPI_SUBSTEPS]

        # Populate target substeps
        target_step = steps[5]  # order=6 → calculate_targets
        target_step.substeps = [SubStep(name=s["name"]) for s in TARGET_SUBSTEPS]

        pipeline = CalibrationPipeline(
            pipeline_id=pipeline_id,
            country=country,
            period=period,
            triggered_by=triggered_by,
            steps=steps,
        )
        self._pipelines[pipeline_id] = pipeline
        return pipeline

    def get_pipeline(self, pipeline_id: str) -> CalibrationPipeline | None:
        return self._pipelines.get(pipeline_id)

    def list_pipelines(
        self,
        country: str | None = None,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[CalibrationPipeline], int]:
        """List pipelines with optional filters."""
        items = list(self._pipelines.values())

        if country:
            items = [p for p in items if p.country == country]
        if status:
            items = [p for p in items if p.status.value == status]

        # Sort by created_at descending
        items.sort(key=lambda p: p.created_at, reverse=True)
        total = len(items)
        return items[offset : offset + limit], total

    def delete_pipeline(self, pipeline_id: str) -> bool:
        return self._pipelines.pop(pipeline_id, None) is not None

    # --- Step advancement ---

    def start_pipeline(self, pipeline_id: str) -> CalibrationPipeline | None:
        pipeline = self._pipelines.get(pipeline_id)
        if not pipeline:
            return None
        pipeline.status = StepStatus.RUNNING
        pipeline.started_at = datetime.utcnow()
        return pipeline

    def start_step(self, pipeline_id: str, step_id: str) -> PipelineStep | None:
        pipeline = self._pipelines.get(pipeline_id)
        if not pipeline:
            return None
        for step in pipeline.steps:
            if step.step_id == step_id:
                step.status = StepStatus.RUNNING
                step.started_at = datetime.utcnow()
                # Also mark the pipeline as running if it's still pending
                if pipeline.status == StepStatus.PENDING:
                    pipeline.status = StepStatus.RUNNING
                    pipeline.started_at = pipeline.started_at or datetime.utcnow()
                return step
        return None

    def complete_step(
        self, pipeline_id: str, step_id: str, error: str | None = None
    ) -> PipelineStep | None:
        pipeline = self._pipelines.get(pipeline_id)
        if not pipeline:
            return None
        for step in pipeline.steps:
            if step.step_id == step_id:
                now = datetime.utcnow()
                step.completed_at = now
                if error:
                    step.status = StepStatus.FAILED
                    step.error = error
                else:
                    step.status = StepStatus.COMPLETED
                if step.started_at:
                    step.duration_seconds = (now - step.started_at).total_seconds()

                # Check if all steps completed → mark pipeline done
                self._check_pipeline_completion(pipeline)
                return step
        return None

    def skip_step(self, pipeline_id: str, step_id: str) -> PipelineStep | None:
        pipeline = self._pipelines.get(pipeline_id)
        if not pipeline:
            return None
        for step in pipeline.steps:
            if step.step_id == step_id:
                step.status = StepStatus.SKIPPED
                self._check_pipeline_completion(pipeline)
                return step
        return None

    def update_substep(
        self,
        pipeline_id: str,
        step_id: str,
        substep_name: str,
        status: StepStatus,
        rows_affected: int | None = None,
        error: str | None = None,
    ) -> SubStep | None:
        pipeline = self._pipelines.get(pipeline_id)
        if not pipeline:
            return None
        for step in pipeline.steps:
            if step.step_id == step_id:
                for ss in step.substeps:
                    if ss.name == substep_name:
                        now = datetime.utcnow()
                        if status == StepStatus.RUNNING and ss.started_at is None:
                            ss.started_at = now
                        if status in (StepStatus.COMPLETED, StepStatus.FAILED):
                            ss.completed_at = now
                        ss.status = status
                        if rows_affected is not None:
                            ss.rows_affected = rows_affected
                        if error:
                            ss.error = error
                        return ss
        return None

    def set_step_run_id(
        self, pipeline_id: str, step_id: str, run_id: int
    ) -> PipelineStep | None:
        """Attach a Databricks run ID to a step for polling."""
        pipeline = self._pipelines.get(pipeline_id)
        if not pipeline:
            return None
        for step in pipeline.steps:
            if step.step_id == step_id:
                step.databricks_run_id = run_id
                return step
        return None

    # --- Databricks polling ---

    def poll_databricks_status(
        self, pipeline_id: str, client: DatabricksClient
    ) -> list[dict[str, Any]]:
        """Poll Databricks for status of steps with attached run IDs.

        Returns a list of updates applied (for logging/debugging).
        """
        pipeline = self._pipelines.get(pipeline_id)
        if not pipeline:
            return []

        updates: list[dict[str, Any]] = []

        for step in pipeline.steps:
            if step.databricks_run_id is None:
                continue
            if step.status in (StepStatus.COMPLETED, StepStatus.FAILED, StepStatus.SKIPPED):
                continue

            try:
                run = client.client.jobs.get_run(step.databricks_run_id)
                state = run.state

                if state and state.life_cycle_state:
                    lcs = state.life_cycle_state.value
                    result_state = state.result_state.value if state.result_state else None

                    new_status = self._map_databricks_state(lcs, result_state)
                    error_msg = state.state_message if new_status == StepStatus.FAILED else None

                    if new_status != step.status:
                        if new_status == StepStatus.RUNNING:
                            self.start_step(pipeline_id, step.step_id)
                        elif new_status in (StepStatus.COMPLETED, StepStatus.FAILED):
                            self.complete_step(pipeline_id, step.step_id, error=error_msg)

                        updates.append({
                            "step_id": step.step_id,
                            "run_id": step.databricks_run_id,
                            "old_status": step.status.value,
                            "new_status": new_status.value,
                        })

            except Exception as e:
                logger.warning(
                    f"Failed to poll Databricks run {step.databricks_run_id} "
                    f"for step {step.step_id}: {e}"
                )

        return updates

    # --- Internal helpers ---

    @staticmethod
    def _map_databricks_state(
        lifecycle: str, result: str | None
    ) -> StepStatus:
        """Map Databricks run lifecycle/result to our StepStatus."""
        if lifecycle in ("PENDING", "QUEUED", "BLOCKED"):
            return StepStatus.PENDING
        if lifecycle in ("RUNNING", "TERMINATING"):
            return StepStatus.RUNNING
        if lifecycle in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            if result == "SUCCESS":
                return StepStatus.COMPLETED
            if result in ("FAILED", "TIMEDOUT", "CANCELED"):
                return StepStatus.FAILED
            if lifecycle == "SKIPPED":
                return StepStatus.SKIPPED
            # TERMINATED without result → assume completed
            return StepStatus.COMPLETED
        return StepStatus.PENDING

    def _check_pipeline_completion(self, pipeline: CalibrationPipeline) -> None:
        """Check if all steps are done and update pipeline status accordingly."""
        terminal = {StepStatus.COMPLETED, StepStatus.FAILED, StepStatus.SKIPPED}
        if all(s.status in terminal for s in pipeline.steps):
            pipeline.completed_at = datetime.utcnow()
            if any(s.status == StepStatus.FAILED for s in pipeline.steps):
                pipeline.status = StepStatus.FAILED
                failed = [s.step_id for s in pipeline.steps if s.status == StepStatus.FAILED]
                pipeline.error = f"Steps failed: {', '.join(failed)}"
            else:
                pipeline.status = StepStatus.COMPLETED


# Module-level singleton
pipeline_tracker = PipelineTracker()
