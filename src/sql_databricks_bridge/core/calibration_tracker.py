"""In-memory tracker for calibration pipeline steps.

Manages the lifecycle of calibration steps attached to trigger jobs.
Each trigger job can optionally have calibration steps tracked here.
"""
from __future__ import annotations

import logging
from datetime import datetime

from sql_databricks_bridge.models.calibration import (
    CALIBRATION_STEP_ORDER,
    CalibrationJobInfo,
    CalibrationStep,
    CalibrationStepName,
    build_calibration_steps,
)

logger = logging.getLogger(__name__)


class CalibrationTracker:
    """Tracks calibration pipeline step progression per trigger job.

    Thread-safety note: FastAPI runs on a single event loop, so dict
    mutations are safe without locks for the async handler context.
    """

    def __init__(self) -> None:
        self._jobs: dict[str, CalibrationJobInfo] = {}

    # --- Create / Get / Delete ---

    def create(
        self,
        job_id: str,
        country: str = "",
        period: str | None = None,
        aggregations: dict[str, bool] | None = None,
    ) -> CalibrationJobInfo:
        """Create calibration step tracking for a trigger job."""
        info = CalibrationJobInfo(
            job_id=job_id,
            country=country,
            period=period,
            aggregations=aggregations or {},
            steps=build_calibration_steps(),
        )
        self._jobs[job_id] = info
        logger.info("Calibration steps created for job %s", job_id)
        return info

    def get(self, job_id: str) -> CalibrationJobInfo | None:
        return self._jobs.get(job_id)

    def delete(self, job_id: str) -> bool:
        return self._jobs.pop(job_id, None) is not None

    # --- Step lifecycle ---

    def start_step(self, job_id: str, step_name: CalibrationStepName) -> CalibrationStep | None:
        """Mark a step as running with current timestamp.

        If the step is already running, this is a no-op (preserves the
        original started_at timestamp).
        """
        info = self._jobs.get(job_id)
        if not info:
            return None
        step = info.get_step(step_name)
        if not step:
            return None
        if step.status == "running":
            return step
        step.status = "running"
        step.started_at = datetime.utcnow()
        logger.debug("Job %s step %s → running", job_id, step_name)
        return step

    def complete_step(
        self, job_id: str, step_name: CalibrationStepName, error: str | None = None
    ) -> CalibrationStep | None:
        """Mark a step as completed or failed."""
        info = self._jobs.get(job_id)
        if not info:
            return None
        step = info.get_step(step_name)
        if not step:
            return None
        step.completed_at = datetime.utcnow()
        if error:
            step.status = "failed"
            step.error = error
        else:
            step.status = "completed"
        logger.debug("Job %s step %s → %s", job_id, step_name, step.status)
        return step

    def cancel_job(self, job_id: str) -> bool:
        """Cancel all running/pending steps for a job. Returns True if job was found."""
        info = self._jobs.get(job_id)
        if not info:
            return False
        now = datetime.utcnow()
        for step in info.steps:
            if step.status in ("pending", "running"):
                step.status = "cancelled"
                step.completed_at = now
        logger.info("Cancelled all steps for job %s", job_id)
        return True

    def fail_remaining(self, job_id: str, from_step: CalibrationStepName, error: str) -> None:
        """When a step fails, mark it and leave subsequent steps as pending."""
        info = self._jobs.get(job_id)
        if not info:
            return
        # The failed step should already be marked via complete_step
        # No need to fail subsequent steps - they just stay pending
        # This matches the mock server behavior

    def attach_run_id(
        self, job_id: str, step_name: CalibrationStepName, run_id: int
    ) -> None:
        """Attach a Databricks job run ID to a step for polling."""
        info = self._jobs.get(job_id)
        if not info:
            return
        step = info.get_step(step_name)
        if step:
            step.databricks_run_id = run_id
            logger.info("Job %s step %s → run_id=%d", job_id, step_name, run_id)

    def get_steps_with_run_ids(self) -> list[tuple[str, CalibrationStepName, int]]:
        """Return all (job_id, step_name, run_id) for steps being polled."""
        result = []
        for job_id, info in self._jobs.items():
            for step in info.steps:
                if (
                    step.databricks_run_id is not None
                    and step.status in ("pending", "running", "completed")
                ):
                    result.append((job_id, step.name, step.databricks_run_id))
        return result

    def advance_after_sync(self, job_id: str) -> CalibrationStepName | None:
        """After sync_data completes, start the next step (copy_to_calibration).
        Returns the name of the step that was started, or None."""
        info = self._jobs.get(job_id)
        if not info:
            return None
        sync_step = info.get_step("sync_data")
        if not sync_step or sync_step.status != "completed":
            return None
        # Start next step
        next_name: CalibrationStepName = "copy_to_calibration"
        self.start_step(job_id, next_name)
        return next_name

    # --- Serialization helpers ---

    def update_step_tasks(
        self, job_id: str, step_name: CalibrationStepName, tasks: list
    ) -> None:
        """Update the Databricks sub-task list for a step."""
        info = self._jobs.get(job_id)
        if not info:
            return
        step = info.get_step(step_name)
        if step:
            step.tasks = tasks

    def get_steps_for_response(self, job_id: str) -> list[dict] | None:
        """Get steps as dicts for API response serialization."""
        info = self._jobs.get(job_id)
        if not info:
            return None
        return [
            {
                "name": s.name,
                "status": s.status,
                "started_at": s.started_at.isoformat() if s.started_at else None,
                "completed_at": s.completed_at.isoformat() if s.completed_at else None,
                "error": s.error,
                "tasks": [
                    {
                        "task_key": t.task_key,
                        "status": t.status,
                        "started_at": t.started_at.isoformat() if t.started_at else None,
                        "completed_at": t.completed_at.isoformat() if t.completed_at else None,
                        "error": t.error,
                    }
                    for t in s.tasks
                ],
            }
            for s in info.steps
        ]

    def get_current_step(self, job_id: str) -> CalibrationStepName | None:
        """Get the current running step name for API response."""
        info = self._jobs.get(job_id)
        if not info:
            return None
        return info.current_step


# Module-level singleton
calibration_tracker = CalibrationTracker()
