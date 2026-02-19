"""Unit tests for CalibrationTracker."""
import pytest
from sql_databricks_bridge.core.calibration_tracker import CalibrationTracker
from sql_databricks_bridge.models.calibration import CALIBRATION_STEP_ORDER


@pytest.fixture
def tracker():
    """Fresh tracker for each test."""
    return CalibrationTracker()


class TestCreate:
    def test_create_job_returns_info(self, tracker):
        info = tracker.create("job-1")
        assert info.job_id == "job-1"
        assert len(info.steps) == 6

    def test_create_job_with_country(self, tracker):
        info = tracker.create("job-1", country="bolivia")
        assert info.country == "bolivia"

    def test_create_job_all_steps_pending(self, tracker):
        info = tracker.create("job-1")
        for step in info.steps:
            assert step.status == "pending"
            assert step.started_at is None
            assert step.completed_at is None
            assert step.error is None

    def test_create_job_step_names_match_frontend(self, tracker):
        info = tracker.create("job-1")
        names = [s.name for s in info.steps]
        assert names == list(CALIBRATION_STEP_ORDER)

    def test_create_multiple_jobs(self, tracker):
        tracker.create("job-1")
        tracker.create("job-2")
        assert tracker.get("job-1") is not None
        assert tracker.get("job-2") is not None


class TestStepLifecycle:
    def test_start_step(self, tracker):
        tracker.create("job-1")
        step = tracker.start_step("job-1", "sync_data")
        assert step is not None
        assert step.status == "running"
        assert step.started_at is not None

    def test_complete_step_success(self, tracker):
        tracker.create("job-1")
        tracker.start_step("job-1", "sync_data")
        step = tracker.complete_step("job-1", "sync_data")
        assert step.status == "completed"
        assert step.completed_at is not None
        assert step.error is None

    def test_complete_step_with_error(self, tracker):
        tracker.create("job-1")
        tracker.start_step("job-1", "sync_data")
        step = tracker.complete_step("job-1", "sync_data", error="Query timeout")
        assert step.status == "failed"
        assert step.error == "Query timeout"

    def test_start_nonexistent_job(self, tracker):
        result = tracker.start_step("nope", "sync_data")
        assert result is None

    def test_complete_nonexistent_job(self, tracker):
        result = tracker.complete_step("nope", "sync_data")
        assert result is None


class TestCurrentStep:
    def test_current_step_none_when_all_pending(self, tracker):
        tracker.create("job-1")
        assert tracker.get_current_step("job-1") is None

    def test_current_step_returns_running(self, tracker):
        tracker.create("job-1")
        tracker.start_step("job-1", "sync_data")
        assert tracker.get_current_step("job-1") == "sync_data"

    def test_current_step_none_after_completion(self, tracker):
        tracker.create("job-1")
        tracker.start_step("job-1", "sync_data")
        tracker.complete_step("job-1", "sync_data")
        assert tracker.get_current_step("job-1") is None

    def test_current_step_advances(self, tracker):
        tracker.create("job-1")
        tracker.start_step("job-1", "sync_data")
        tracker.complete_step("job-1", "sync_data")
        tracker.start_step("job-1", "copy_to_calibration")
        assert tracker.get_current_step("job-1") == "copy_to_calibration"


class TestAdvanceAfterSync:
    def test_advance_starts_copy(self, tracker):
        tracker.create("job-1")
        tracker.start_step("job-1", "sync_data")
        tracker.complete_step("job-1", "sync_data")
        next_step = tracker.advance_after_sync("job-1")
        assert next_step == "copy_to_calibration"
        assert tracker.get_current_step("job-1") == "copy_to_calibration"

    def test_advance_does_nothing_if_sync_not_completed(self, tracker):
        tracker.create("job-1")
        tracker.start_step("job-1", "sync_data")
        next_step = tracker.advance_after_sync("job-1")
        assert next_step is None


class TestRunIds:
    def test_attach_run_id(self, tracker):
        tracker.create("job-1")
        tracker.attach_run_id("job-1", "copy_to_calibration", 12345)
        info = tracker.get("job-1")
        step = info.get_step("copy_to_calibration")
        assert step.databricks_run_id == 12345

    def test_get_steps_with_run_ids(self, tracker):
        tracker.create("job-1")
        tracker.attach_run_id("job-1", "copy_to_calibration", 111)
        tracker.attach_run_id("job-1", "merge_data", 222)
        result = tracker.get_steps_with_run_ids()
        assert len(result) == 2
        assert ("job-1", "copy_to_calibration", 111) in result
        assert ("job-1", "merge_data", 222) in result

    def test_completed_steps_not_in_run_ids(self, tracker):
        tracker.create("job-1")
        tracker.attach_run_id("job-1", "copy_to_calibration", 111)
        tracker.start_step("job-1", "copy_to_calibration")
        tracker.complete_step("job-1", "copy_to_calibration")
        result = tracker.get_steps_with_run_ids()
        assert len(result) == 0


class TestSerialization:
    def test_get_steps_for_response(self, tracker):
        tracker.create("job-1")
        tracker.start_step("job-1", "sync_data")
        steps = tracker.get_steps_for_response("job-1")
        assert len(steps) == 6
        assert steps[0]["name"] == "sync_data"
        assert steps[0]["status"] == "running"
        assert steps[0]["started_at"] is not None
        assert steps[1]["status"] == "pending"

    def test_get_steps_for_unknown_job(self, tracker):
        assert tracker.get_steps_for_response("nope") is None


class TestDelete:
    def test_delete_existing(self, tracker):
        tracker.create("job-1")
        assert tracker.delete("job-1") is True
        assert tracker.get("job-1") is None

    def test_delete_nonexistent(self, tracker):
        assert tracker.delete("nope") is False
