"""Unit tests for calibration step integration in trigger endpoints."""
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from sql_databricks_bridge.auth.authorized_users import AuthorizedUser
from sql_databricks_bridge.api.routes.trigger import (
    CalibrationStepResponse,
    EventSummary,
)


@pytest.fixture
def admin_user():
    return AuthorizedUser(
        email="admin@test.com",
        name="Admin",
        roles=["admin"],
        countries=["*"],
    )


def _make_client(user: AuthorizedUser) -> TestClient:
    """Create a TestClient with auth dependency overridden."""
    from sql_databricks_bridge.api.dependencies import get_current_azure_ad_user
    from sql_databricks_bridge.api.routes import trigger as trigger_module
    from sql_databricks_bridge.api.routes.trigger import _trigger_jobs

    _trigger_jobs.clear()
    # Ensure no launcher leaks from lifespan into isolated tests
    saved_launcher = trigger_module._calibration_launcher
    trigger_module._calibration_launcher = None

    with patch("sql_databricks_bridge.main._event_poller", None):
        from sql_databricks_bridge.main import app

        mock_db_client = MagicMock()
        mock_db_client.execute_sql.return_value = []
        app.state.databricks_client = mock_db_client

        app.dependency_overrides[get_current_azure_ad_user] = lambda: user

        client = TestClient(app)
        yield client

        app.dependency_overrides.pop(get_current_azure_ad_user, None)
        _trigger_jobs.clear()
        trigger_module._calibration_launcher = saved_launcher


@pytest.fixture
def admin_client(admin_user):
    yield from _make_client(admin_user)


def _make_mock_job(job_id: str, country: str = "bolivia", queries: list[str] | None = None):
    """Create a properly configured mock ExtractionJob for trigger tests.

    TestClient runs background tasks synchronously, so the mock must have
    all attributes that _run_trigger_extraction reads.
    """
    mock_job = MagicMock()
    mock_job.job_id = job_id
    mock_job.country = country
    mock_job.queries = queries or ["j_atoscompra_new"]
    mock_job.created_at = datetime.utcnow()
    mock_job.status = "pending"
    mock_job.started_at = None
    mock_job.completed_at = None
    mock_job.error = None
    mock_job.results = []
    mock_job.current_query = None
    mock_job.chunk_size = 10000
    mock_job.queries_completed = 0
    mock_job.queries_failed = 0
    return mock_job


class TestTriggerCreatesSteps:
    @patch("sql_databricks_bridge.api.routes.trigger.update_job_status")
    @patch("sql_databricks_bridge.api.routes.trigger.insert_job")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_trigger_creates_calibration_steps(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockInsertJob,
        MockUpdateStatus,
        admin_client,
    ):
        """POST /trigger creates 6 calibration steps.

        TestClient runs background tasks synchronously, so by the time
        we inspect state, sync_data has completed and advance_after_sync
        has started copy_to_calibration.
        """
        from sql_databricks_bridge.core.calibration_tracker import calibration_tracker

        mock_extractor = MagicMock()
        mock_extractor.execute_query.return_value = iter([])  # no chunks
        mock_job = _make_mock_job("cal-test-001")
        mock_extractor.create_job.return_value = mock_job
        MockExtractor.return_value = mock_extractor

        response = admin_client.post("/api/v1/trigger", json={
            "country": "bolivia",
            "stage": "calibracion",
        })

        assert response.status_code == 201
        job_id = response.json()["job_id"]

        # Verify calibration steps were created
        info = calibration_tracker.get(job_id)
        assert info is not None
        assert len(info.steps) == 6

        # Background task ran: sync_data completed, copy_to_calibration auto-started
        sync = info.get_step("sync_data")
        assert sync.status == "completed"
        copy = info.get_step("copy_to_calibration")
        assert copy.status == "running"

        calibration_tracker.delete(job_id)

    @patch("sql_databricks_bridge.api.routes.trigger.update_job_status")
    @patch("sql_databricks_bridge.api.routes.trigger.insert_job")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_trigger_remaining_steps_pending(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockInsertJob,
        MockUpdateStatus,
        admin_client,
    ):
        """After sync completes, only copy_to_calibration advances; rest stay pending."""
        from sql_databricks_bridge.core.calibration_tracker import calibration_tracker

        mock_extractor = MagicMock()
        mock_extractor.execute_query.return_value = iter([])
        mock_job = _make_mock_job("cal-test-002")
        mock_extractor.create_job.return_value = mock_job
        MockExtractor.return_value = mock_extractor

        response = admin_client.post("/api/v1/trigger", json={
            "country": "bolivia",
            "stage": "calibracion",
        })

        assert response.status_code == 201
        job_id = response.json()["job_id"]

        info = calibration_tracker.get(job_id)
        # steps[0]=sync_data (completed), steps[1]=copy_to_calibration (running)
        # steps[2:] should all still be pending
        for step in info.steps[2:]:
            assert step.status == "pending", f"{step.name} should be pending, got {step.status}"

        calibration_tracker.delete(job_id)

    @patch("sql_databricks_bridge.api.routes.trigger.update_job_status")
    @patch("sql_databricks_bridge.api.routes.trigger.insert_job")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_trigger_sync_failure_marks_step_failed(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockInsertJob,
        MockUpdateStatus,
        admin_client,
    ):
        """When extraction raises at the top level, sync_data is marked failed."""
        from sql_databricks_bridge.core.calibration_tracker import calibration_tracker
        from sql_databricks_bridge.api.schemas import JobStatus

        mock_extractor = MagicMock()
        mock_extractor.execute_query.return_value = iter([])
        mock_job = _make_mock_job("cal-test-003")
        # Simulate: all queries failed, none completed
        mock_job.queries_failed = 1
        mock_job.queries_completed = 0
        mock_extractor.create_job.return_value = mock_job
        MockExtractor.return_value = mock_extractor

        response = admin_client.post("/api/v1/trigger", json={
            "country": "bolivia",
            "stage": "calibracion",
        })

        assert response.status_code == 201
        job_id = response.json()["job_id"]

        info = calibration_tracker.get(job_id)
        sync = info.get_step("sync_data")
        assert sync.status == "failed"
        assert "failed during sync" in sync.error

        calibration_tracker.delete(job_id)


class TestEventSummaryIncludesSteps:
    def test_event_summary_has_steps_field(self):
        """Verify the EventSummary schema includes steps."""
        fields = EventSummary.model_fields
        assert "steps" in fields
        assert "current_step" in fields

    def test_calibration_step_response_schema(self):
        """Verify CalibrationStepResponse has correct fields."""
        step = CalibrationStepResponse(
            name="sync_data",
            status="running",
            started_at=datetime.utcnow(),
        )
        data = step.model_dump()
        assert data["name"] == "sync_data"
        assert data["status"] == "running"
        assert data["started_at"] is not None
        assert data["completed_at"] is None
        assert data["error"] is None

    def test_calibration_step_response_failed(self):
        """Verify CalibrationStepResponse with error."""
        step = CalibrationStepResponse(
            name="merge_data",
            status="failed",
            started_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
            error="Table not found",
        )
        data = step.model_dump()
        assert data["status"] == "failed"
        assert data["error"] == "Table not found"
        assert data["completed_at"] is not None
