"""Unit tests for DatabricksJobMonitor."""
import pytest
from unittest.mock import MagicMock, patch, call

from sql_databricks_bridge.core.databricks_monitor import (
    DatabricksJobMonitor,
    map_databricks_state,
)
from sql_databricks_bridge.core.calibration_tracker import CalibrationTracker
from sql_databricks_bridge.core.calibration_launcher import CalibrationJobLauncher


class TestMapDatabricksState:
    def test_pending_states(self):
        assert map_databricks_state("PENDING", None) == "pending"
        assert map_databricks_state("QUEUED", None) == "pending"
        assert map_databricks_state("BLOCKED", None) == "pending"

    def test_running_states(self):
        assert map_databricks_state("RUNNING", None) == "running"
        assert map_databricks_state("TERMINATING", None) == "running"

    def test_terminated_success(self):
        assert map_databricks_state("TERMINATED", "SUCCESS") == "completed"

    def test_terminated_failed(self):
        assert map_databricks_state("TERMINATED", "FAILED") == "failed"
        assert map_databricks_state("TERMINATED", "TIMEDOUT") == "failed"
        assert map_databricks_state("TERMINATED", "CANCELED") == "failed"

    def test_terminated_no_result(self):
        assert map_databricks_state("TERMINATED", None) == "completed"

    def test_internal_error(self):
        assert map_databricks_state("INTERNAL_ERROR", "FAILED") == "failed"
        assert map_databricks_state("INTERNAL_ERROR", None) == "completed"

    def test_skipped(self):
        assert map_databricks_state("SKIPPED", None) == "completed"

    def test_unknown(self):
        assert map_databricks_state("UNKNOWN_STATE", None) == "pending"


class TestDatabricksJobMonitor:
    @pytest.fixture
    def tracker(self):
        """Fresh tracker for each test."""
        return CalibrationTracker()

    @pytest.fixture
    def mock_client(self):
        """Mock DatabricksClient."""
        return MagicMock()

    @pytest.fixture
    def monitor(self, mock_client):
        """Monitor with mock client."""
        return DatabricksJobMonitor(mock_client, poll_interval=1.0)

    def _make_run(self, lifecycle, result_state=None, state_message=None):
        """Create a mock Databricks run response."""
        run = MagicMock()
        run.state.life_cycle_state.value = lifecycle
        if result_state:
            run.state.result_state.value = result_state
        else:
            run.state.result_state = None
        run.state.state_message = state_message
        return run

    def test_poll_run_updates_running(self, monitor, mock_client, tracker):
        """Test that a RUNNING Databricks run updates the step to running."""
        with patch("sql_databricks_bridge.core.databricks_monitor.calibration_tracker", tracker):
            tracker.create("job-1")
            tracker.attach_run_id("job-1", "copy_to_calibration", 100)
            mock_client.client.jobs.get_run.return_value = self._make_run("RUNNING")

            monitor._poll_run("job-1", "copy_to_calibration", 100)

            info = tracker.get("job-1")
            step = info.get_step("copy_to_calibration")
            assert step.status == "running"

    def test_poll_run_updates_completed(self, monitor, mock_client, tracker):
        """Test that a TERMINATED/SUCCESS run updates the step to completed."""
        with patch("sql_databricks_bridge.core.databricks_monitor.calibration_tracker", tracker):
            tracker.create("job-1")
            tracker.attach_run_id("job-1", "copy_to_calibration", 100)
            tracker.start_step("job-1", "copy_to_calibration")
            mock_client.client.jobs.get_run.return_value = self._make_run("TERMINATED", "SUCCESS")

            monitor._poll_run("job-1", "copy_to_calibration", 100)

            info = tracker.get("job-1")
            step = info.get_step("copy_to_calibration")
            assert step.status == "completed"

    def test_poll_run_updates_failed(self, monitor, mock_client, tracker):
        """Test that a TERMINATED/FAILED run updates the step to failed."""
        with patch("sql_databricks_bridge.core.databricks_monitor.calibration_tracker", tracker):
            tracker.create("job-1")
            tracker.attach_run_id("job-1", "copy_to_calibration", 100)
            tracker.start_step("job-1", "copy_to_calibration")
            mock_client.client.jobs.get_run.return_value = self._make_run(
                "TERMINATED", "FAILED", "Job crashed"
            )

            monitor._poll_run("job-1", "copy_to_calibration", 100)

            info = tracker.get("job-1")
            step = info.get_step("copy_to_calibration")
            assert step.status == "failed"
            assert step.error == "Job crashed"

    def test_poll_run_auto_advances(self, monitor, mock_client, tracker):
        """Test that completing a step auto-advances to the next."""
        with patch("sql_databricks_bridge.core.databricks_monitor.calibration_tracker", tracker):
            tracker.create("job-1")
            tracker.attach_run_id("job-1", "copy_to_calibration", 100)
            tracker.start_step("job-1", "copy_to_calibration")
            mock_client.client.jobs.get_run.return_value = self._make_run("TERMINATED", "SUCCESS")

            monitor._poll_run("job-1", "copy_to_calibration", 100)

            info = tracker.get("job-1")
            next_step = info.get_step("merge_data")
            assert next_step.status == "running"

    def test_poll_once_skips_when_no_runs(self, monitor, tracker):
        """Test that _poll_once does nothing when no steps have run IDs."""
        with patch("sql_databricks_bridge.core.databricks_monitor.calibration_tracker", tracker):
            tracker.create("job-1")
            monitor._poll_once()  # Should not raise

    def test_poll_run_handles_sdk_error(self, monitor, mock_client, tracker):
        """Test that SDK errors are handled gracefully."""
        with patch("sql_databricks_bridge.core.databricks_monitor.calibration_tracker", tracker):
            tracker.create("job-1")
            tracker.attach_run_id("job-1", "copy_to_calibration", 100)
            mock_client.client.jobs.get_run.side_effect = Exception("Connection timeout")

            # Should not raise
            monitor._poll_once()

            info = tracker.get("job-1")
            step = info.get_step("copy_to_calibration")
            assert step.status == "pending"  # Unchanged

    def test_poll_single_job(self, monitor, mock_client, tracker):
        """Test polling all steps for a single job."""
        with patch("sql_databricks_bridge.core.databricks_monitor.calibration_tracker", tracker):
            tracker.create("job-1")
            tracker.attach_run_id("job-1", "copy_to_calibration", 100)
            tracker.attach_run_id("job-1", "merge_data", 200)
            mock_client.client.jobs.get_run.return_value = self._make_run("RUNNING")

            updates = monitor.poll_single_job("job-1")

            assert len(updates) == 2


class TestMonitorWithLauncher:
    """Tests for DatabricksJobMonitor integration with CalibrationJobLauncher."""

    @pytest.fixture
    def tracker(self):
        return CalibrationTracker()

    @pytest.fixture
    def mock_client(self):
        return MagicMock()

    @pytest.fixture
    def mock_launcher(self):
        launcher = MagicMock(spec=CalibrationJobLauncher)
        launcher.get_covered_steps.return_value = ["copy_to_calibration"]
        launcher.launch_step.return_value = 999
        return launcher

    @pytest.fixture
    def monitor(self, mock_client, mock_launcher):
        return DatabricksJobMonitor(mock_client, poll_interval=1.0, launcher=mock_launcher)

    def _make_run(self, lifecycle, result_state=None, state_message=None):
        run = MagicMock()
        run.state.life_cycle_state.value = lifecycle
        if result_state:
            run.state.result_state.value = result_state
        else:
            run.state.result_state = None
        run.state.state_message = state_message
        return run

    def test_advance_launches_next_step(self, monitor, mock_client, mock_launcher, tracker):
        """When copy_to_calibration completes, launcher should launch merge_data."""
        with patch("sql_databricks_bridge.core.databricks_monitor.calibration_tracker", tracker):
            tracker.create("job-1", country="bolivia")
            tracker.attach_run_id("job-1", "copy_to_calibration", 100)
            tracker.start_step("job-1", "copy_to_calibration")
            mock_client.client.jobs.get_run.return_value = self._make_run("TERMINATED", "SUCCESS")

            monitor._poll_run("job-1", "copy_to_calibration", 100)

            mock_launcher.launch_step.assert_called_once_with("job-1", "merge_data", "bolivia")

    def test_advance_with_multi_step_coverage(self, monitor, mock_client, mock_launcher, tracker):
        """When merge_data completes and covers 3 steps, all are completed
        and the launcher is called for download_csv."""
        mock_launcher.get_covered_steps.return_value = [
            "merge_data", "simulate_kpis", "calculate_penetration",
        ]
        with patch("sql_databricks_bridge.core.databricks_monitor.calibration_tracker", tracker):
            tracker.create("job-1", country="bolivia")
            tracker.attach_run_id("job-1", "merge_data", 200)
            tracker.start_step("job-1", "merge_data")
            mock_client.client.jobs.get_run.return_value = self._make_run("TERMINATED", "SUCCESS")

            monitor._poll_run("job-1", "merge_data", 200)

            info = tracker.get("job-1")
            assert info.get_step("merge_data").status == "completed"
            assert info.get_step("simulate_kpis").status == "completed"
            assert info.get_step("calculate_penetration").status == "completed"
            # download_csv should be started and launcher called
            assert info.get_step("download_csv").status == "running"
            mock_launcher.launch_step.assert_called_once_with("job-1", "download_csv", "bolivia")

    def test_no_launcher_still_advances(self, mock_client, tracker):
        """Without a launcher, steps still advance (just no job submission)."""
        monitor = DatabricksJobMonitor(mock_client, poll_interval=1.0, launcher=None)
        with patch("sql_databricks_bridge.core.databricks_monitor.calibration_tracker", tracker):
            tracker.create("job-1", country="bolivia")
            tracker.attach_run_id("job-1", "copy_to_calibration", 100)
            tracker.start_step("job-1", "copy_to_calibration")

            run = MagicMock()
            run.state.life_cycle_state.value = "TERMINATED"
            run.state.result_state.value = "SUCCESS"
            mock_client.client.jobs.get_run.return_value = run

            monitor._poll_run("job-1", "copy_to_calibration", 100)

            info = tracker.get("job-1")
            assert info.get_step("copy_to_calibration").status == "completed"
            assert info.get_step("merge_data").status == "running"
