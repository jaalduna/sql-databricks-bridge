"""Launches Databricks jobs for calibration pipeline steps.

Maps calibration step names to Databricks Asset Bundle jobs and triggers
them via the Jobs API.  A single Databricks job may cover multiple
calibration steps (e.g. the country penetration job covers merge_data,
simulate_kpis, and calculate_penetration).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from sql_databricks_bridge.core.calibration_tracker import calibration_tracker
from sql_databricks_bridge.models.calibration import CalibrationStepName

logger = logging.getLogger(__name__)


@dataclass
class StepJobSpec:
    """Describes the Databricks job backing a calibration step."""

    job_name: str
    """Display-name substring used to locate the DAB job (e.g. 'Bronze Copy')."""

    parameters: dict[str, str] = field(default_factory=dict)
    """Static job parameters (placeholders ``{country}`` are resolved at launch time)."""

    covers_steps: list[CalibrationStepName] = field(default_factory=list)
    """Steps covered by this single Databricks run.

    When the run completes, *all* covered steps are marked completed and the
    launcher advances to the step immediately after the last covered one.
    """


# Default step → job mapping.  Country-specific job names use ``{country}``
# as a placeholder that gets resolved at launch time.
DEFAULT_STEP_JOBS: dict[CalibrationStepName, StepJobSpec | None] = {
    "sync_data": None,  # handled by the trigger endpoint directly
    "copy_to_calibration": StepJobSpec(
        job_name="Bronze Copy",
        parameters={
            "country": "{country}",
            "source_catalog": "000-sql-databricks-bridge",
            "target_catalog": "001-calibration-3-0",
        },
        covers_steps=["copy_to_calibration"],
    ),
    "merge_data": StepJobSpec(
        job_name="{Country} Penetration Calibration",
        parameters={
            "start_period": "202301",
            "final_period": "0",
        },
        covers_steps=["merge_data", "simulate_kpis", "calculate_penetration"],
    ),
    "simulate_kpis": None,       # covered by merge_data's job
    "calculate_penetration": None,  # covered by merge_data's job
    "download_csv": None,  # auto-complete, not a Databricks job
}


class CalibrationJobLauncher:
    """Resolves calibration steps to Databricks jobs and submits them.

    Usage::

        launcher = CalibrationJobLauncher(databricks_client)
        run_id = launcher.launch_step("job-123", "copy_to_calibration", "bolivia")
    """

    def __init__(
        self,
        databricks_client,
        step_jobs: dict[CalibrationStepName, StepJobSpec | None] | None = None,
    ) -> None:
        self._client = databricks_client
        self._step_jobs = step_jobs or dict(DEFAULT_STEP_JOBS)
        # Cache: resolved job_name → Databricks numeric job_id
        self._job_id_cache: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def launch_step(
        self,
        job_id: str,
        step_name: CalibrationStepName,
        country: str,
        extra_params: dict[str, str] | None = None,
    ) -> int | None:
        """Launch the Databricks job for *step_name*.

        Returns the Databricks ``run_id``, or ``None`` if the step does not
        need a Databricks job (auto-complete steps, or steps covered by an
        earlier step's job).
        """
        spec = self._step_jobs.get(step_name)
        if spec is None:
            # Step doesn't have its own job.  If it's download_csv,
            # auto-complete it.
            if step_name == "download_csv":
                calibration_tracker.start_step(job_id, step_name)
                calibration_tracker.complete_step(job_id, step_name)
                logger.info("Job %s step %s auto-completed (no Databricks job)", job_id, step_name)
            return None

        resolved_name = self._resolve_job_name(spec.job_name, country)
        db_job_id = self._find_job_id(resolved_name)
        if db_job_id is None:
            error = f"Databricks job not found: '{resolved_name}'"
            logger.error("Job %s step %s: %s", job_id, step_name, error)
            calibration_tracker.complete_step(job_id, step_name, error=error)
            return None

        params = self._resolve_params(spec.parameters, country, extra_params)

        try:
            run_id = self._client.run_job(db_job_id, params)
        except Exception as exc:
            error = f"Failed to trigger Databricks job {db_job_id}: {exc}"
            logger.error("Job %s step %s: %s", job_id, step_name, error)
            calibration_tracker.complete_step(job_id, step_name, error=error)
            return None

        # Attach run_id to the *first* covered step (the one being launched)
        calibration_tracker.attach_run_id(job_id, step_name, run_id)
        logger.info(
            "Job %s step %s → Databricks run %d (job_id=%d, name='%s')",
            job_id, step_name, run_id, db_job_id, resolved_name,
        )
        return run_id

    def get_covered_steps(self, step_name: CalibrationStepName) -> list[CalibrationStepName]:
        """Return the list of steps covered by *step_name*'s Databricks job.

        If the step doesn't have a spec, returns ``[step_name]``.
        """
        spec = self._step_jobs.get(step_name)
        if spec is None or not spec.covers_steps:
            return [step_name]
        return list(spec.covers_steps)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_job_name(template: str, country: str) -> str:
        """Replace ``{country}`` and ``{Country}`` placeholders."""
        return template.replace("{country}", country).replace("{Country}", country.capitalize())

    def _find_job_id(self, resolved_name: str) -> int | None:
        """Find the Databricks numeric job ID for *resolved_name*, using cache."""
        if resolved_name in self._job_id_cache:
            return self._job_id_cache[resolved_name]
        job_id = self._client.find_job_by_name(resolved_name)
        if job_id is not None:
            self._job_id_cache[resolved_name] = job_id
        return job_id

    @staticmethod
    def _resolve_params(
        template: dict[str, str],
        country: str,
        extra: dict[str, str] | None,
    ) -> dict[str, str]:
        """Resolve ``{country}`` placeholders in parameter values."""
        params = {k: v.replace("{country}", country) for k, v in template.items()}
        if extra:
            params.update(extra)
        return params
