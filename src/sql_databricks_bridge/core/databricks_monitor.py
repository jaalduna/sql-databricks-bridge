"""Background monitor that polls Databricks Jobs API for calibration step status.

Periodically checks all calibration steps that have a databricks_run_id attached
and updates their status based on the Databricks run lifecycle state.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from sql_databricks_bridge.core.calibration_tracker import calibration_tracker
from sql_databricks_bridge.models.calibration import CalibrationStepName, DatabricksTaskStatus

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
        if result_state in ("FAILED", "TIMEDOUT", "CANCELED", "MAXIMUM_CONCURRENT_RUNS_REACHED",
                            "UPSTREAM_FAILED", "UPSTREAM_CANCELED"):
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
        """Poll a single Databricks run and update the calibration step.

        Reads both the top-level run state and individual task states from
        ``run.tasks`` to provide granular progress to the frontend.
        """
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

        # Check if per-task tracking is active for this step
        has_task_step_map = False
        if self._launcher:
            spec = self._launcher._step_jobs.get(step_name)
            if spec and spec.task_step_map:
                has_task_step_map = True

        # Track per-task → per-step transitions (must run before _update_task_progress)
        self._track_task_steps(job_id, step_name, run)

        # Update per-task progress regardless of top-level status change
        self._update_task_progress(job_id, step_name, run)

        # When task_step_map is active, per-task tracking drives step transitions.
        # Only handle terminal run states (completed/failed) here for advance/finalize.
        if has_task_step_map:
            if new_status == "completed":
                # Guard: check if we already advanced past this run's covered steps.
                # Since completed steps are now polled, this prevents double-advancing.
                covered = self._launcher.get_covered_steps(step_name) if self._launcher else [step_name]
                last_covered = covered[-1]
                from sql_databricks_bridge.models.calibration import CALIBRATION_STEP_ORDER
                try:
                    last_idx = CALIBRATION_STEP_ORDER.index(last_covered)
                except ValueError:
                    return
                if last_idx + 1 < len(CALIBRATION_STEP_ORDER):
                    next_step_name = CALIBRATION_STEP_ORDER[last_idx + 1]
                    next_step_obj = info.get_step(next_step_name)
                    if next_step_obj and next_step_obj.status in ("running", "completed", "failed"):
                        # Already advanced — nothing to do
                        return
                else:
                    # Last step in the pipeline; check if all steps already done
                    all_done = all(s.status in ("completed", "failed") for s in info.steps)
                    if all_done:
                        return

                # Run terminated successfully — advance past the last covered step
                logger.info(
                    "Job %s step %s run %d: TERMINATED/SUCCESS — advancing pipeline",
                    job_id, step_name, run_id,
                )
                self._advance_next_step(job_id, step_name)
            elif new_status == "failed":
                error_msg = run.state.state_message if run.state else "Databricks run failed"
                # Fail any covered steps that are still pending/running
                covered = self._launcher.get_covered_steps(step_name) if self._launcher else [step_name]
                for s in covered:
                    s_obj = info.get_step(s)
                    if s_obj and s_obj.status not in ("completed", "failed"):
                        calibration_tracker.complete_step(job_id, s, error=error_msg)
                self._persist_steps_to_sqlite(job_id)
                self._finalize_job(job_id, status="failed", error=f"Step {step_name} failed: {error_msg}")
            # For running/pending states: per-task tracking handles it, do nothing
            return

        # Legacy path: no task_step_map, use top-level run status directly
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
            self._persist_steps_to_sqlite(job_id)
            # Finalize the overall job as failed
            self._finalize_job(job_id, status="failed", error=f"Step {step_name} failed: {error_msg}")

    def _track_task_steps(self, job_id: str, step_name: CalibrationStepName, run) -> None:
        """Track per-task completion and map to calibration steps.

        When a StepJobSpec has a task_step_map, each Databricks task completion
        drives the corresponding calibration step transition. This provides
        granular progress instead of all covered steps completing at once.
        """
        if not self._launcher:
            return
        spec = self._launcher._step_jobs.get(step_name)
        if not spec or not spec.task_step_map:
            return

        run_tasks = getattr(run, "tasks", None)
        if not run_tasks:
            return

        info = calibration_tracker.get(job_id)
        if not info:
            return

        for task in run_tasks:
            task_key = getattr(task, "task_key", None)
            if not task_key:
                continue

            mapped_step = spec.task_step_map.get(task_key)
            if not mapped_step:
                continue

            step_obj = info.get_step(mapped_step)
            if not step_obj:
                continue

            task_state = getattr(task, "state", None)
            if not task_state:
                continue

            t_lifecycle = task_state.life_cycle_state.value if task_state.life_cycle_state else "PENDING"
            t_result = task_state.result_state.value if task_state.result_state else None
            t_status = map_databricks_state(t_lifecycle, t_result)

            # Start step when task starts running
            if t_status == "running" and step_obj.status == "pending":
                calibration_tracker.start_step(job_id, mapped_step)
                logger.info(
                    "Job %s task %s → step %s started (per-task tracking)",
                    job_id, task_key, mapped_step,
                )

            # Complete step when task completes
            elif t_status == "completed" and step_obj.status not in ("completed", "failed"):
                calibration_tracker.complete_step(job_id, mapped_step)
                logger.info(
                    "Job %s task %s → step %s completed (per-task tracking)",
                    job_id, task_key, mapped_step,
                )

            # Fail step when task fails
            elif t_status == "failed" and step_obj.status not in ("completed", "failed"):
                error_msg = task_state.state_message if task_state else f"Task {task_key} failed"
                calibration_tracker.complete_step(job_id, mapped_step, error=error_msg)
                logger.info(
                    "Job %s task %s → step %s failed (per-task tracking)",
                    job_id, task_key, mapped_step,
                )

    def _update_task_progress(self, job_id: str, step_name: CalibrationStepName, run) -> None:
        """Extract per-task status from run.tasks and update the step's task list."""
        run_tasks = getattr(run, "tasks", None)
        if not run_tasks:
            return

        # Get task_key_map from launcher spec (if available)
        task_key_map: dict[str, str] = {}
        if self._launcher:
            spec = self._launcher._step_jobs.get(step_name)
            if spec:
                task_key_map = spec.task_key_map

        task_statuses: list[DatabricksTaskStatus] = []
        for task in run_tasks:
            task_key = getattr(task, "task_key", None)
            if not task_key:
                continue

            task_state = getattr(task, "state", None)
            if not task_state:
                task_statuses.append(DatabricksTaskStatus(task_key=task_key))
                continue

            t_lifecycle = task_state.life_cycle_state.value if task_state.life_cycle_state else "PENDING"
            t_result = task_state.result_state.value if task_state.result_state else None
            t_status = map_databricks_state(t_lifecycle, t_result)

            # Extract timestamps from task
            t_started = None
            t_completed = None
            start_time_ms = getattr(task, "start_time", None)
            end_time_ms = getattr(task, "end_time", None)
            if start_time_ms:
                t_started = datetime.utcfromtimestamp(start_time_ms / 1000)
            if end_time_ms:
                t_completed = datetime.utcfromtimestamp(end_time_ms / 1000)

            t_error = None
            if t_status == "failed" and task_state.state_message:
                t_error = task_state.state_message

            # Use mapped label if available, otherwise raw task_key
            display_key = task_key_map.get(task_key, task_key)

            task_statuses.append(DatabricksTaskStatus(
                task_key=display_key,
                status=t_status,
                started_at=t_started,
                completed_at=t_completed,
                error=t_error,
            ))

        calibration_tracker.update_step_tasks(job_id, step_name, task_statuses)

    @staticmethod
    def _persist_steps_to_sqlite(job_id: str) -> None:
        """Persist current calibration steps to SQLite (best-effort)."""
        try:
            from sql_databricks_bridge.api.routes.trigger import _sqlite_db_path, _persist_steps

            if _sqlite_db_path:
                _persist_steps(job_id, _sqlite_db_path)
        except Exception as e:
            logger.debug("Failed to persist steps for job %s: %s", job_id, e)

    def _advance_next_step(self, job_id: str, completed_step: CalibrationStepName) -> None:
        """After a step completes, auto-complete any additionally covered
        steps, then start and launch the next step."""
        from sql_databricks_bridge.models.calibration import CALIBRATION_STEP_ORDER

        # Determine which steps are covered by the completed step's job
        covered: list[CalibrationStepName] = [completed_step]
        if self._launcher:
            covered = self._launcher.get_covered_steps(completed_step)

        info = calibration_tracker.get(job_id)
        if not info:
            return

        # Check if task_step_map drove individual step tracking.
        # If so, skip the auto-complete loop — steps were already transitioned
        # individually by _track_task_steps().
        #
        # However, if the run's actual tasks didn't match the map (e.g. a country
        # with the old monolithic single-task job), fall back to auto-complete so
        # the pipeline doesn't stall.
        use_task_step_map = False
        if self._launcher:
            spec = self._launcher._step_jobs.get(completed_step)
            if spec and spec.task_step_map:
                # Verify the map actually fired: check if covered steps beyond
                # the completed one were already transitioned (not still pending).
                extra_steps = [s for s in covered if s != completed_step]
                if extra_steps:
                    all_transitioned = all(
                        info.get_step(s) and info.get_step(s).status in ("running", "completed", "failed")
                        for s in extra_steps
                    )
                    use_task_step_map = all_transitioned
                else:
                    use_task_step_map = True  # no extra steps to check

        if not use_task_step_map:
            # Auto-complete all covered steps (including the root step itself)
            for extra in covered:
                step_obj = info.get_step(extra)
                if step_obj and step_obj.status in ("completed", "failed"):
                    continue  # already transitioned
                calibration_tracker.start_step(job_id, extra)
                calibration_tracker.complete_step(job_id, extra)
                logger.info("Job %s step %s auto-completed (covered by %s)", job_id, extra, completed_step)
        else:
            # Per-task tracking drove extra steps, but the root step itself may
            # still be stuck as "running" if no Databricks task mapped to it.
            root_obj = info.get_step(completed_step)
            if root_obj and root_obj.status not in ("completed", "failed"):
                calibration_tracker.complete_step(job_id, completed_step)
                logger.info("Job %s step %s completed (root step finalized)", job_id, completed_step)

        # Persist step changes to SQLite
        self._persist_steps_to_sqlite(job_id)

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
                extra_params: dict[str, str] = {}
                if info and info.period:
                    extra_params["final_period"] = info.period
                if info and info.aggregations:
                    for key, value in info.aggregations.items():
                        if value:
                            extra_params[key] = "true"
                self._launcher.launch_step(
                    job_id, next_step, country, extra_params=extra_params or None,
                )

        # Persist step changes after launch
        self._persist_steps_to_sqlite(job_id)

        # If launch_step auto-completed the step (e.g. download_csv has no
        # Databricks job), advance again so the pipeline doesn't stall.
        step_info = calibration_tracker.get(job_id)
        if step_info:
            step_obj = step_info.get_step(next_step)
            if step_obj and step_obj.status == "completed":
                self._advance_next_step(job_id, next_step)

    def _finalize_job(self, job_id: str, status: str = "completed", error: str | None = None) -> None:
        """Update the overall job status when calibration finishes.

        Updates three stores:
        1. Databricks Delta table (persistent, shared)
        2. In-memory _trigger_jobs record (for live polling)
        3. SQLite local store (for crash recovery)
        """
        completed_at = datetime.utcnow()

        # 1) Delta table
        try:
            from sql_databricks_bridge.core.config import get_settings
            from sql_databricks_bridge.db.jobs_table import update_job_status

            settings = get_settings()
            update_job_status(
                self._client, settings.jobs_table, job_id, status,
                completed_at=completed_at, error=error,
            )
        except Exception as e:
            logger.warning("Failed to finalize job %s in Delta: %s", job_id, e)

        # 2) In-memory record + SQLite (so frontend polling sees the final state)
        try:
            from sql_databricks_bridge.api.routes.trigger import finalize_trigger_job

            finalize_trigger_job(job_id, status, completed_at, error)
        except Exception as e:
            logger.warning("Failed to finalize trigger job %s in memory/SQLite: %s", job_id, e)

        logger.info("Job %s finalized as %s", job_id, status)

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
            if step.status == "failed":
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
