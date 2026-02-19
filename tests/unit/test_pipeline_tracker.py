"""Unit tests for PipelineTracker -- calibration pipeline state management."""

from unittest.mock import MagicMock, patch

import pytest

from sql_databricks_bridge.core.pipeline_tracker import PipelineTracker
from sql_databricks_bridge.models.pipeline import (
    CalibrationPipeline,
    PipelineStep,
    StepStatus,
    SubStep,
    build_calibration_steps,
)


@pytest.fixture
def tracker():
    return PipelineTracker()


class TestBuildCalibrationSteps:
    """Tests for the standard step factory."""

    def test_returns_six_steps(self):
        steps = build_calibration_steps()
        assert len(steps) == 6

    def test_step_order_is_sequential(self):
        steps = build_calibration_steps()
        orders = [s.order for s in steps]
        assert orders == [1, 2, 3, 4, 5, 6]

    def test_step_ids(self):
        steps = build_calibration_steps()
        ids = [s.step_id for s in steps]
        assert ids == [
            "sync",
            "copy_bronze",
            "merge",
            "simulate_weights",
            "simulate_kpis",
            "calculate_targets",
        ]

    def test_all_steps_start_pending(self):
        steps = build_calibration_steps()
        assert all(s.status == StepStatus.PENDING for s in steps)


class TestCreatePipeline:
    """Tests for pipeline creation."""

    def test_create_pipeline_basic(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        assert p.country == "bolivia"
        assert p.status == StepStatus.PENDING
        assert len(p.steps) == 6
        assert p.pipeline_id

    def test_create_pipeline_with_period(self, tracker):
        p = tracker.create_pipeline(country="chile", period="202602")
        assert p.period == "202602"

    def test_create_pipeline_with_sync_queries(self, tracker):
        queries = ["j_atoscompra_new", "hato_cabecalho"]
        p = tracker.create_pipeline(country="bolivia", sync_queries=queries)
        sync_step = p.steps[0]
        assert sync_step.step_id == "sync"
        assert len(sync_step.substeps) == 2
        assert sync_step.substeps[0].name == "j_atoscompra_new"
        assert sync_step.substeps[1].name == "hato_cabecalho"

    def test_kpi_substeps_populated(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        kpi_step = p.steps[4]  # simulate_kpis
        assert kpi_step.step_id == "simulate_kpis"
        assert len(kpi_step.substeps) == 3
        names = [s.name for s in kpi_step.substeps]
        assert names == ["kpi_all", "kpi_bc", "kpi_original"]

    def test_target_substeps_populated(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        target_step = p.steps[5]  # calculate_targets
        assert target_step.step_id == "calculate_targets"
        assert len(target_step.substeps) == 2
        names = [s.name for s in target_step.substeps]
        assert names == ["penetration_targets", "volume_targets"]

    def test_pipeline_stored(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        assert tracker.get_pipeline(p.pipeline_id) is p


class TestListPipelines:
    """Tests for listing pipelines."""

    def test_list_empty(self, tracker):
        items, total = tracker.list_pipelines()
        assert items == []
        assert total == 0

    def test_list_all(self, tracker):
        tracker.create_pipeline(country="bolivia")
        tracker.create_pipeline(country="chile")
        items, total = tracker.list_pipelines()
        assert total == 2
        assert len(items) == 2

    def test_list_filter_by_country(self, tracker):
        tracker.create_pipeline(country="bolivia")
        tracker.create_pipeline(country="chile")
        items, total = tracker.list_pipelines(country="bolivia")
        assert total == 1
        assert items[0].country == "bolivia"

    def test_list_filter_by_status(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        tracker.create_pipeline(country="chile")
        tracker.start_pipeline(p.pipeline_id)
        items, total = tracker.list_pipelines(status="running")
        assert total == 1
        assert items[0].status == StepStatus.RUNNING

    def test_list_pagination(self, tracker):
        for _ in range(5):
            tracker.create_pipeline(country="bolivia")
        items, total = tracker.list_pipelines(limit=2, offset=0)
        assert total == 5
        assert len(items) == 2

        items2, _ = tracker.list_pipelines(limit=2, offset=2)
        assert len(items2) == 2


class TestStepAdvancement:
    """Tests for starting, completing, and skipping steps."""

    def test_start_step(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        step = tracker.start_step(p.pipeline_id, "sync")
        assert step is not None
        assert step.status == StepStatus.RUNNING
        assert step.started_at is not None
        # Pipeline should also be running
        assert p.status == StepStatus.RUNNING

    def test_complete_step(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        tracker.start_step(p.pipeline_id, "sync")
        step = tracker.complete_step(p.pipeline_id, "sync")
        assert step is not None
        assert step.status == StepStatus.COMPLETED
        assert step.completed_at is not None
        assert step.duration_seconds is not None
        assert step.duration_seconds >= 0

    def test_fail_step(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        tracker.start_step(p.pipeline_id, "sync")
        step = tracker.complete_step(p.pipeline_id, "sync", error="Connection lost")
        assert step.status == StepStatus.FAILED
        assert step.error == "Connection lost"

    def test_skip_step(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        step = tracker.skip_step(p.pipeline_id, "simulate_weights")
        assert step.status == StepStatus.SKIPPED

    def test_invalid_pipeline_id(self, tracker):
        assert tracker.start_step("nonexistent", "sync") is None
        assert tracker.complete_step("nonexistent", "sync") is None
        assert tracker.skip_step("nonexistent", "sync") is None

    def test_invalid_step_id(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        assert tracker.start_step(p.pipeline_id, "nonexistent") is None

    def test_pipeline_completes_when_all_steps_done(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        for step in p.steps:
            tracker.start_step(p.pipeline_id, step.step_id)
            tracker.complete_step(p.pipeline_id, step.step_id)
        assert p.status == StepStatus.COMPLETED
        assert p.completed_at is not None

    def test_pipeline_fails_when_any_step_fails(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        # Complete first 3 steps
        for step in p.steps[:3]:
            tracker.start_step(p.pipeline_id, step.step_id)
            tracker.complete_step(p.pipeline_id, step.step_id)
        # Fail the 4th
        tracker.start_step(p.pipeline_id, "simulate_weights")
        tracker.complete_step(p.pipeline_id, "simulate_weights", error="No data")
        # Skip/complete the rest
        tracker.skip_step(p.pipeline_id, "simulate_kpis")
        tracker.skip_step(p.pipeline_id, "calculate_targets")

        assert p.status == StepStatus.FAILED
        assert "simulate_weights" in p.error


class TestSubStepUpdates:
    """Tests for substep status updates."""

    def test_update_substep_running(self, tracker):
        p = tracker.create_pipeline(
            country="bolivia", sync_queries=["q1", "q2"]
        )
        ss = tracker.update_substep(p.pipeline_id, "sync", "q1", StepStatus.RUNNING)
        assert ss is not None
        assert ss.status == StepStatus.RUNNING
        assert ss.started_at is not None

    def test_update_substep_completed(self, tracker):
        p = tracker.create_pipeline(
            country="bolivia", sync_queries=["q1", "q2"]
        )
        tracker.update_substep(p.pipeline_id, "sync", "q1", StepStatus.RUNNING)
        ss = tracker.update_substep(
            p.pipeline_id, "sync", "q1", StepStatus.COMPLETED, rows_affected=1000
        )
        assert ss.status == StepStatus.COMPLETED
        assert ss.rows_affected == 1000
        assert ss.completed_at is not None

    def test_update_substep_failed(self, tracker):
        p = tracker.create_pipeline(
            country="bolivia", sync_queries=["q1"]
        )
        ss = tracker.update_substep(
            p.pipeline_id, "sync", "q1", StepStatus.FAILED, error="Timeout"
        )
        assert ss.status == StepStatus.FAILED
        assert ss.error == "Timeout"

    def test_update_substep_nonexistent(self, tracker):
        p = tracker.create_pipeline(country="bolivia", sync_queries=["q1"])
        assert tracker.update_substep(p.pipeline_id, "sync", "nonexistent", StepStatus.RUNNING) is None
        assert tracker.update_substep(p.pipeline_id, "nonexistent_step", "q1", StepStatus.RUNNING) is None

    def test_step_progress_pct(self, tracker):
        p = tracker.create_pipeline(
            country="bolivia", sync_queries=["q1", "q2", "q3", "q4"]
        )
        sync_step = p.steps[0]
        assert sync_step.progress_pct == 0.0

        tracker.update_substep(p.pipeline_id, "sync", "q1", StepStatus.COMPLETED)
        assert sync_step.progress_pct == 25.0

        tracker.update_substep(p.pipeline_id, "sync", "q2", StepStatus.COMPLETED)
        assert sync_step.progress_pct == 50.0


class TestDatabricksRunId:
    """Tests for attaching and tracking Databricks run IDs."""

    def test_set_run_id(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        step = tracker.set_step_run_id(p.pipeline_id, "merge", 12345)
        assert step is not None
        assert step.databricks_run_id == 12345

    def test_set_run_id_invalid_pipeline(self, tracker):
        assert tracker.set_step_run_id("nonexistent", "merge", 123) is None

    def test_set_run_id_invalid_step(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        assert tracker.set_step_run_id(p.pipeline_id, "nonexistent", 123) is None


class TestDatabricksPolling:
    """Tests for polling Databricks job run status."""

    def _mock_run(self, lifecycle: str, result: str | None = None, message: str = ""):
        """Build a mock Databricks run object."""
        run = MagicMock()
        run.state.life_cycle_state.value = lifecycle
        if result:
            run.state.result_state.value = result
        else:
            run.state.result_state = None
        run.state.state_message = message
        return run

    def test_poll_running_step(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        tracker.set_step_run_id(p.pipeline_id, "merge", 99)

        mock_client = MagicMock()
        mock_client.client.jobs.get_run.return_value = self._mock_run("RUNNING")

        updates = tracker.poll_databricks_status(p.pipeline_id, mock_client)
        assert len(updates) == 1
        assert updates[0]["new_status"] == "running"

        merge_step = p.steps[2]
        assert merge_step.status == StepStatus.RUNNING

    def test_poll_completed_step(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        tracker.set_step_run_id(p.pipeline_id, "merge", 99)
        tracker.start_step(p.pipeline_id, "merge")

        mock_client = MagicMock()
        mock_client.client.jobs.get_run.return_value = self._mock_run(
            "TERMINATED", "SUCCESS"
        )

        updates = tracker.poll_databricks_status(p.pipeline_id, mock_client)
        assert len(updates) == 1
        assert updates[0]["new_status"] == "completed"

        merge_step = p.steps[2]
        assert merge_step.status == StepStatus.COMPLETED

    def test_poll_failed_step(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        tracker.set_step_run_id(p.pipeline_id, "merge", 99)
        tracker.start_step(p.pipeline_id, "merge")

        mock_client = MagicMock()
        mock_client.client.jobs.get_run.return_value = self._mock_run(
            "TERMINATED", "FAILED", "OOM error"
        )

        updates = tracker.poll_databricks_status(p.pipeline_id, mock_client)
        assert len(updates) == 1
        assert updates[0]["new_status"] == "failed"

    def test_poll_skips_completed_steps(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        tracker.set_step_run_id(p.pipeline_id, "merge", 99)
        tracker.start_step(p.pipeline_id, "merge")
        tracker.complete_step(p.pipeline_id, "merge")

        mock_client = MagicMock()
        updates = tracker.poll_databricks_status(p.pipeline_id, mock_client)
        assert len(updates) == 0
        # Should not even call get_run
        mock_client.client.jobs.get_run.assert_not_called()

    def test_poll_nonexistent_pipeline(self, tracker):
        mock_client = MagicMock()
        updates = tracker.poll_databricks_status("nonexistent", mock_client)
        assert updates == []

    def test_poll_handles_api_error_gracefully(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        tracker.set_step_run_id(p.pipeline_id, "merge", 99)

        mock_client = MagicMock()
        mock_client.client.jobs.get_run.side_effect = Exception("API unavailable")

        # Should not raise
        updates = tracker.poll_databricks_status(p.pipeline_id, mock_client)
        assert updates == []

    def test_poll_no_update_when_status_unchanged(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        tracker.set_step_run_id(p.pipeline_id, "merge", 99)
        # Step is PENDING, Databricks says PENDING → no change

        mock_client = MagicMock()
        mock_client.client.jobs.get_run.return_value = self._mock_run("PENDING")

        updates = tracker.poll_databricks_status(p.pipeline_id, mock_client)
        assert updates == []


class TestDeletePipeline:
    """Tests for pipeline deletion."""

    def test_delete_existing(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        assert tracker.delete_pipeline(p.pipeline_id) is True
        assert tracker.get_pipeline(p.pipeline_id) is None

    def test_delete_nonexistent(self, tracker):
        assert tracker.delete_pipeline("nonexistent") is False


class TestPipelineProgress:
    """Tests for pipeline progress calculation."""

    def test_pipeline_progress_zero(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        assert p.progress_pct == 0.0

    def test_pipeline_progress_partial(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        tracker.start_step(p.pipeline_id, "sync")
        tracker.complete_step(p.pipeline_id, "sync")
        # 1/6 completed
        assert p.progress_pct == pytest.approx(16.7)

    def test_pipeline_progress_full(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        for step in p.steps:
            tracker.start_step(p.pipeline_id, step.step_id)
            tracker.complete_step(p.pipeline_id, step.step_id)
        assert p.progress_pct == 100.0

    def test_current_step_tracking(self, tracker):
        p = tracker.create_pipeline(country="bolivia")
        assert p.current_step is None

        tracker.start_step(p.pipeline_id, "sync")
        assert p.current_step.step_id == "sync"

        tracker.complete_step(p.pipeline_id, "sync")
        assert p.current_step is None

        tracker.start_step(p.pipeline_id, "copy_bronze")
        assert p.current_step.step_id == "copy_bronze"
