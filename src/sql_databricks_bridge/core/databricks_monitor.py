"""Background monitor that polls Databricks Jobs API for calibration step status.

Periodically checks all calibration steps that have a databricks_run_id attached
and updates their status based on the Databricks run lifecycle state.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from sql_databricks_bridge.core.calibration_tracker import calibration_tracker
from sql_databricks_bridge.models.calibration import CalibrationStepName

logger = logging.getLogger(__name__)

# Databricks run lifecycle → calibration step status
_LIFECYCLE_MAP: dict[str, str] = {
    "PENDING": "pending",
    "QUEUED": "pending",
    "BLOCKED": "pending",
    "RUNNING": "running",
    "TERMINATING": "running",
}


def map_databricks_state(lifecycle: str, result_state: str | None) -> str:
    """Map Databricks run lifecycle + result_state to our step status.

    Args:
        lifecycle: Databricks LifecycleState value (PENDING, RUNNING, TERMINATED, etc.)
        result_state: Databricks ResultState value (SUCCESS, FAILED, CANCELED, etc.) or None

    Returns:
        Step status string: pending, running, completed, or failed
    """
    if lifecycle in _LIFECYCLE_MAP:
        return _LIFECYCLE_MAP[lifecycle]

    if lifecycle in ("TERMINATED", "INTERNAL_ERROR"):
        if result_state == "SUCCESS":
            return "completed"
        if result_state in ("FAILED", "TIMEDOUT", "CANCELED", "MAXIMUM_CONCURRENT_RUNS_REACHED"):
            return "failed"
        # TERMINATED without result → assume completed
        return "completed"

    if lifecycle == "SKIPPED":
        return "completed"  # Map skipped to completed for simplicity

    return "pending"


class DatabricksJobMonitor:
    """Polls Databricks Jobs API for calibration step run status.

    Usage:
        monitor = DatabricksJobMonitor(databricks_client, poll_interval=10)
        task = asyncio.create_task(monitor.start())
        # ... later ...
        monitor.stop()
    """

    def __init__(
        self,
        databricks_client,  # DatabricksClient instance
        poll_interval: float = 10.0,
        launcher=None,  # CalibrationJobLauncher instance (optional)
    ) -> None:
        self._client = databricks_client
        self._poll_interval = poll_interval
        self._running = False
        self._launcher = launcher

    async def start(self) -> None:
        """Start the polling loop."""
        self._running = True
        logger.info("DatabricksJobMonitor started (interval=%.1fs)", self._poll_interval)
        while self._running:
            try:
                # Run sync Databricks API calls in a thread to avoid blocking the event loop
                await asyncio.to_thread(self._poll_once)
            except Exception as e:
                logger.error("Monitor poll error: %s", e)
            await asyncio.sleep(self._poll_interval)

    def stop(self) -> None:
        """Signal the polling loop to stop."""
        self._running = False
        logger.info("DatabricksJobMonitor stopped")

    def _poll_once(self) -> None:
        """Poll all active steps with Databricks run IDs."""
        steps_to_poll = calibration_tracker.get_steps_with_run_ids()
        if not steps_to_poll:
            return

        logger.debug("Polling %d Databricks runs", len(steps_to_poll))

        for job_id, step_name, run_id in steps_to_poll:
            try:
                self._poll_run(job_id, step_name, run_id)
            except Exception as e:
                logger.warning(
                    "Failed to poll run %d for job %s step %s: %s",
                    run_id, job_id, step_name, e,
                )

    def _poll_run(self, job_id: str, step_name: CalibrationStepName, run_id: int) -> None:
        """Poll a single Databricks run and update the calibration step."""
        run = self._client.client.jobs.get_run(run_id)
        if not run.state or not run.state.life_cycle_state:
            return

        lifecycle = run.state.life_cycle_state.value
        result_state = run.state.result_state.value if run.state.result_state else None
        new_status = map_databricks_state(lifecycle, result_state)

        # Get current step status
        info = calibration_tracker.get(job_id)
        if not info:
            return
        step = info.get_step(step_name)
        if not step:
            return

        # Only update if status changed
        if new_status == step.status:
            return

        logger.info(
            "Job %s step %s run %d: %s → %s (lifecycle=%s, result=%s)",
            job_id, step_name, run_id, step.status, new_status, lifecycle, result_state,
        )

        if new_status == "running":
            calibration_tracker.start_step(job_id, step_name)
        elif new_status == "completed":
            error_msg = None
            calibration_tracker.complete_step(job_id, step_name, error=error_msg)
            # Auto-advance to next step
            self._advance_next_step(job_id, step_name)
        elif new_status == "failed":
            error_msg = run.state.state_message if run.state else "Databricks run failed"
            calibration_tracker.complete_step(job_id, step_name, error=error_msg)

    def _advance_next_step(self, job_id: str, completed_step: CalibrationStepName) -> None:
        """After a step completes, auto-complete any additionally covered
        steps, then start and launch the next step."""
        from sql_databricks_bridge.models.calibration import CALIBRATION_STEP_ORDER

        # Determine which steps are covered by the completed step's job
        covered: list[CalibrationStepName] = [completed_step]
        if self._launcher:
            covered = self._launcher.get_covered_steps(completed_step)

        # Auto-complete any extra covered steps beyond the one just completed
        for extra in covered:
            if extra == completed_step:
                continue
            calibration_tracker.start_step(job_id, extra)
            calibration_tracker.complete_step(job_id, extra)
            logger.info("Job %s step %s auto-completed (covered by %s)", job_id, extra, completed_step)

        # Find the step after the last covered step
        last_covered = covered[-1]
        try:
            idx = CALIBRATION_STEP_ORDER.index(last_covered)
        except ValueError:
            return

        if idx + 1 >= len(CALIBRATION_STEP_ORDER):
            logger.info("Job %s: all calibration steps completed", job_id)
            # Update overall job status to completed
            self._finalize_job(job_id)
            return

        next_step = CALIBRATION_STEP_ORDER[idx + 1]
        calibration_tracker.start_step(job_id, next_step)
        logger.info("Job %s auto-advanced to step %s", job_id, next_step)

        # Launch the Databricks job for the next step
        if self._launcher:
            info = calibration_tracker.get(job_id)
            country = info.country if info else ""
            if country:
                self._launcher.launch_step(job_id, next_step, country)

        # If launch_step auto-completed the step (e.g. download_csv has no
        # Databricks job), advance again so the pipeline doesn't stall.
        step_info = calibration_tracker.get(job_id)
        if step_info:
            step_obj = step_info.get_step(next_step)
            if step_obj and step_obj.status == "completed":
                self._advance_next_step(job_id, next_step)

    def _finalize_job(self, job_id: str, status: str = "completed", error: str | None = None) -> None:
        """Update the overall job status in the Delta table when calibration finishes."""
        try:
            from sql_databricks_bridge.core.config import get_settings
            from sql_databricks_bridge.db.jobs_table import update_job_status

            settings = get_settings()
            completed_at = datetime.utcnow()
            update_job_status(
                self._client, settings.jobs_table, job_id, status,
                completed_at=completed_at, error=error,
            )
            logger.info("Job %s finalized as %s", job_id, status)
        except Exception as e:
            logger.warning("Failed to finalize job %s: %s", job_id, e)

    def poll_single_job(self, job_id: str) -> list[dict]:
        """Poll all steps for a single job. Returns list of updates made.

        This is useful for the POST /pipeline/{id}/poll endpoint.
        """
        info = calibration_tracker.get(job_id)
        if not info:
            return []

        updates = []
        for step in info.steps:
            if step.databricks_run_id is None:
                continue
            if step.status in ("completed", "failed"):
                continue
            try:
                old_status = step.status
                self._poll_run(job_id, step.name, step.databricks_run_id)
                if step.status != old_status:
                    updates.append({
                        "step": step.name,
                        "old_status": old_status,
                        "new_status": step.status,
                        "run_id": step.databricks_run_id,
                    })
            except Exception as e:
                logger.warning("Failed to poll run %d: %s", step.databricks_run_id, e)

        return updates
