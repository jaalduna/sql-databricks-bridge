"""Unit tests for CalibrationJobLauncher."""
import pytest
from unittest.mock import MagicMock, patch

from sql_databricks_bridge.core.calibration_launcher import (
    CalibrationJobLauncher,
    StepJobSpec,
    DEFAULT_STEP_JOBS,
)
from sql_databricks_bridge.core.calibration_tracker import CalibrationTracker


@pytest.fixture
def tracker():
    return CalibrationTracker()


@pytest.fixture
def mock_client():
    client = MagicMock()
    client.find_job_by_name.return_value = 42
    client.run_job.return_value = 9001
    return client


@pytest.fixture
def launcher(mock_client):
    return CalibrationJobLauncher(mock_client)


class TestLaunchStep:
    def test_launch_copy_to_calibration(self, launcher, mock_client, tracker):
        with patch("sql_databricks_bridge.core.calibration_launcher.calibration_tracker", tracker):
            tracker.create("job-1", country="bolivia")
            tracker.start_step("job-1", "copy_to_calibration")

            run_id = launcher.launch_step("job-1", "copy_to_calibration", "bolivia")

            assert run_id == 9001
            mock_client.find_job_by_name.assert_called_with("Bronze Copy")
            mock_client.run_job.assert_called_once_with(42, {
                "country": "bolivia",
                "source_catalog": "000-sql-databricks-bridge",
                "target_catalog": "001-calibration-3-0",
            })
            # run_id should be attached to the step
            info = tracker.get("job-1")
            step = info.get_step("copy_to_calibration")
            assert step.databricks_run_id == 9001

    def test_launch_merge_data_resolves_country(self, launcher, mock_client, tracker):
        with patch("sql_databricks_bridge.core.calibration_launcher.calibration_tracker", tracker):
            tracker.create("job-1", country="bolivia")
            tracker.start_step("job-1", "merge_data")

            run_id = launcher.launch_step("job-1", "merge_data", "bolivia")

            assert run_id == 9001
            mock_client.find_job_by_name.assert_called_with("Bolivia Penetration Calibration")

    def test_launch_sync_data_returns_none(self, launcher, tracker):
        """sync_data is handled by the trigger endpoint, not the launcher."""
        with patch("sql_databricks_bridge.core.calibration_launcher.calibration_tracker", tracker):
            tracker.create("job-1", country="bolivia")
            result = launcher.launch_step("job-1", "sync_data", "bolivia")
            assert result is None

    def test_launch_download_csv_auto_completes(self, launcher, tracker):
        with patch("sql_databricks_bridge.core.calibration_launcher.calibration_tracker", tracker):
            tracker.create("job-1", country="bolivia")
            result = launcher.launch_step("job-1", "download_csv", "bolivia")

            assert result is None
            info = tracker.get("job-1")
            step = info.get_step("download_csv")
            assert step.status == "completed"

    def test_launch_simulate_kpis_returns_none(self, launcher, tracker):
        """simulate_kpis is covered by merge_data's job."""
        with patch("sql_databricks_bridge.core.calibration_launcher.calibration_tracker", tracker):
            tracker.create("job-1", country="bolivia")
            result = launcher.launch_step("job-1", "simulate_kpis", "bolivia")
            assert result is None

    def test_launch_job_not_found_marks_failed(self, launcher, mock_client, tracker):
        """If the Databricks job is not found, the step is marked failed."""
        mock_client.find_job_by_name.return_value = None
        with patch("sql_databricks_bridge.core.calibration_launcher.calibration_tracker", tracker):
            tracker.create("job-1", country="bolivia")
            tracker.start_step("job-1", "copy_to_calibration")

            result = launcher.launch_step("job-1", "copy_to_calibration", "bolivia")

            assert result is None
            info = tracker.get("job-1")
            step = info.get_step("copy_to_calibration")
            assert step.status == "failed"
            assert "not found" in step.error

    def test_launch_run_job_error_marks_failed(self, launcher, mock_client, tracker):
        """If run_job raises, the step is marked failed."""
        mock_client.run_job.side_effect = Exception("Connection refused")
        with patch("sql_databricks_bridge.core.calibration_launcher.calibration_tracker", tracker):
            tracker.create("job-1", country="bolivia")
            tracker.start_step("job-1", "copy_to_calibration")

            result = launcher.launch_step("job-1", "copy_to_calibration", "bolivia")

            assert result is None
            info = tracker.get("job-1")
            step = info.get_step("copy_to_calibration")
            assert step.status == "failed"
            assert "Connection refused" in step.error


class TestGetCoveredSteps:
    def test_copy_covers_itself(self, launcher):
        covered = launcher.get_covered_steps("copy_to_calibration")
        assert covered == ["copy_to_calibration"]

    def test_merge_covers_three_steps(self, launcher):
        covered = launcher.get_covered_steps("merge_data")
        assert covered == ["merge_data", "simulate_kpis", "calculate_penetration"]

    def test_unconfigured_step_returns_itself(self, launcher):
        covered = launcher.get_covered_steps("sync_data")
        assert covered == ["sync_data"]

    def test_download_csv_returns_itself(self, launcher):
        covered = launcher.get_covered_steps("download_csv")
        assert covered == ["download_csv"]


class TestResolveJobName:
    def test_country_placeholder(self):
        result = CalibrationJobLauncher._resolve_job_name("{Country} Pipeline", "bolivia")
        assert result == "Bolivia Pipeline"

    def test_lowercase_placeholder(self):
        result = CalibrationJobLauncher._resolve_job_name("{country}_job", "chile")
        assert result == "chile_job"

    def test_no_placeholder(self):
        result = CalibrationJobLauncher._resolve_job_name("Bronze Copy", "any")
        assert result == "Bronze Copy"


class TestJobIdCaching:
    def test_caches_resolved_job_id(self, launcher, mock_client, tracker):
        with patch("sql_databricks_bridge.core.calibration_launcher.calibration_tracker", tracker):
            tracker.create("job-1", country="bolivia")
            tracker.start_step("job-1", "copy_to_calibration")

            launcher.launch_step("job-1", "copy_to_calibration", "bolivia")

            tracker.create("job-2", country="bolivia")
            tracker.start_step("job-2", "copy_to_calibration")
            launcher.launch_step("job-2", "copy_to_calibration", "bolivia")

            # find_job_by_name should only be called once (cached)
            assert mock_client.find_job_by_name.call_count == 1


class TestExtraParams:
    def test_extra_params_merged(self, launcher, mock_client, tracker):
        with patch("sql_databricks_bridge.core.calibration_launcher.calibration_tracker", tracker):
            tracker.create("job-1", country="bolivia")
            tracker.start_step("job-1", "copy_to_calibration")

            launcher.launch_step(
                "job-1", "copy_to_calibration", "bolivia",
                extra_params={"target_catalog": "custom-catalog"},
            )

            call_params = mock_client.run_job.call_args[0][1]
            assert call_params["target_catalog"] == "custom-catalog"
            assert call_params["country"] == "bolivia"
