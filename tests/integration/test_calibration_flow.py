"""Integration test for calibration pipeline flow.

Tests the full lifecycle: trigger -> sync_data -> step progression.
Uses mocked SQL Server and Databricks to validate the internal flow.
"""
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from sql_databricks_bridge.auth.authorized_users import AuthorizedUser
from sql_databricks_bridge.core.calibration_tracker import calibration_tracker


BOLIVIA_QUERIES = [
    "j_atoscompra_new",
    "hato_cabecalho",
    "cl_catproduto_new",
    "cl_painelista",
    "cl_cidades",
]


@pytest.fixture(autouse=True)
def clean_tracker():
    """Clean up calibration tracker and trigger jobs between tests."""
    calibration_tracker._jobs.clear()
    # Also clean the module-level _trigger_jobs dict
    from sql_databricks_bridge.api.routes.trigger import _trigger_jobs
    _trigger_jobs.clear()
    yield
    calibration_tracker._jobs.clear()
    _trigger_jobs.clear()


@pytest.fixture
def admin_user():
    return AuthorizedUser(
        email="integration-test@test.com",
        name="Integration Tester",
        roles=["admin"],
        countries=["*"],
    )


@pytest.fixture
def test_client(admin_user):
    """Create test client with mocked auth and database dependencies."""
    from sql_databricks_bridge.api.dependencies import get_current_azure_ad_user
    from sql_databricks_bridge.main import app

    mock_db_client = MagicMock()
    mock_db_client.execute_sql.return_value = []
    app.state.databricks_client = mock_db_client

    app.dependency_overrides[get_current_azure_ad_user] = lambda: admin_user
    client = TestClient(app, raise_server_exceptions=False)
    yield client
    app.dependency_overrides.pop(get_current_azure_ad_user, None)


class TestCalibrationPipelineFlow:
    """Integration tests for the full calibration pipeline flow."""

    @patch("sql_databricks_bridge.api.routes.trigger.insert_job")
    @patch("sql_databricks_bridge.api.routes.trigger.update_job_status")
    @patch("sql_databricks_bridge.api.routes.trigger.build_tag", return_value="bolivia-integration-test")
    @patch("sql_databricks_bridge.api.routes.trigger.insert_version_tag")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_trigger_returns_steps_in_event_detail(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockInsertVersionTag,
        MockBuildTag,
        MockUpdateJob,
        MockInsertJob,
        test_client,
    ):
        """GET /events/{job_id} should return calibration steps."""
        import polars as pl
        from sql_databricks_bridge.core.extractor import ExtractionJob

        job = ExtractionJob(
            job_id="integ-001",
            country="bolivia",
            queries=BOLIVIA_QUERIES,
        )
        mock_extractor = MagicMock()
        mock_extractor.create_job.return_value = job
        mock_extractor.execute_query.return_value = [
            pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        ]
        MockExtractor.return_value = mock_extractor

        MockDeltaWriter.return_value.resolve_table_name.return_value = "kpi_prd_01.bridge.bolivia_test"
        MockDeltaWriter.return_value.get_current_version.return_value = 1

        # Trigger -- TestClient runs background tasks synchronously
        response = test_client.post("/api/v1/trigger", json={
            "country": "bolivia",
            "stage": "calibracion",
            "queries": BOLIVIA_QUERIES,
        })
        assert response.status_code == 202
        job_id = response.json()["job_id"]

        # Get event detail -- the in-flight record should have steps
        detail = test_client.get(f"/api/v1/events/{job_id}")
        assert detail.status_code == 200
        data = detail.json()

        # Verify steps are present
        assert "steps" in data
        assert data["steps"] is not None
        assert len(data["steps"]) == 6

        # Verify step names match frontend contract
        step_names = [s["name"] for s in data["steps"]]
        expected = [
            "sync_data",
            "copy_to_calibration",
            "merge_data",
            "simulate_kpis",
            "calculate_penetration",
            "download_csv",
        ]
        assert step_names == expected

        # Verify current_step is present
        assert "current_step" in data

    @patch("sql_databricks_bridge.api.routes.trigger.insert_job")
    @patch("sql_databricks_bridge.api.routes.trigger.update_job_status")
    @patch("sql_databricks_bridge.api.routes.trigger.build_tag", return_value="bolivia-integration-test")
    @patch("sql_databricks_bridge.api.routes.trigger.insert_version_tag")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_sync_completion_advances_to_copy(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockInsertVersionTag,
        MockBuildTag,
        MockUpdateJob,
        MockInsertJob,
        test_client,
    ):
        """After sync_data completes, copy_to_calibration should start."""
        import polars as pl
        from sql_databricks_bridge.core.extractor import ExtractionJob

        job = ExtractionJob(
            job_id="integ-002",
            country="bolivia",
            queries=["j_atoscompra_new"],
        )
        mock_extractor = MagicMock()
        mock_extractor.create_job.return_value = job
        mock_extractor.execute_query.return_value = [
            pl.DataFrame({"id": [1]})
        ]
        MockExtractor.return_value = mock_extractor
        MockDeltaWriter.return_value.resolve_table_name.return_value = "kpi_prd_01.bridge.bolivia_test"
        MockDeltaWriter.return_value.get_current_version.return_value = 1

        # Trigger
        response = test_client.post("/api/v1/trigger", json={
            "country": "bolivia",
            "stage": "calibracion",
            "queries": ["j_atoscompra_new"],
        })
        job_id = response.json()["job_id"]

        # Get event detail -- background task ran synchronously
        detail = test_client.get(f"/api/v1/events/{job_id}").json()

        steps = {s["name"]: s for s in detail["steps"]}

        # sync_data should be completed (background task ran)
        assert steps["sync_data"]["status"] == "completed"
        assert steps["sync_data"]["completed_at"] is not None

        # copy_to_calibration should be running (auto-advanced after sync)
        assert steps["copy_to_calibration"]["status"] == "running"

        # current_step should be copy_to_calibration
        assert detail["current_step"] == "copy_to_calibration"

        # Remaining steps should be pending
        assert steps["merge_data"]["status"] == "pending"
        assert steps["simulate_kpis"]["status"] == "pending"
        assert steps["calculate_penetration"]["status"] == "pending"
        assert steps["download_csv"]["status"] == "pending"

    @patch("sql_databricks_bridge.api.routes.trigger.insert_job")
    @patch("sql_databricks_bridge.api.routes.trigger.update_job_status")
    @patch("sql_databricks_bridge.api.routes.trigger.build_tag", return_value="bolivia-integration-test")
    @patch("sql_databricks_bridge.api.routes.trigger.insert_version_tag")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_sync_failure_marks_step_failed(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockInsertVersionTag,
        MockBuildTag,
        MockUpdateJob,
        MockInsertJob,
        test_client,
    ):
        """If sync_data extraction fails entirely, step should be marked failed."""
        from sql_databricks_bridge.core.extractor import ExtractionJob

        job = ExtractionJob(
            job_id="integ-003",
            country="bolivia",
            queries=["j_atoscompra_new"],
        )
        mock_extractor = MagicMock()
        mock_extractor.create_job.return_value = job
        mock_extractor.execute_query.side_effect = Exception("Connection refused")
        MockExtractor.return_value = mock_extractor

        # Trigger
        response = test_client.post("/api/v1/trigger", json={
            "country": "bolivia",
            "stage": "calibracion",
            "queries": ["j_atoscompra_new"],
        })
        job_id = response.json()["job_id"]

        # Get event detail
        detail = test_client.get(f"/api/v1/events/{job_id}").json()

        steps = {s["name"]: s for s in detail["steps"]}

        # sync_data should be failed -- the single query failed, so
        # queries_failed > 0 and queries_completed == 0 -> step marked failed
        assert steps["sync_data"]["status"] == "failed"
        assert steps["sync_data"]["error"] is not None

        # No step should have advanced
        assert steps["copy_to_calibration"]["status"] == "pending"

    @patch("sql_databricks_bridge.api.routes.trigger.insert_job")
    @patch("sql_databricks_bridge.api.routes.trigger.update_job_status")
    @patch("sql_databricks_bridge.api.routes.trigger.build_tag", return_value="bolivia-integration-test")
    @patch("sql_databricks_bridge.api.routes.trigger.insert_version_tag")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_events_list_includes_steps(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockInsertVersionTag,
        MockBuildTag,
        MockUpdateJob,
        MockInsertJob,
        test_client,
    ):
        """GET /events should include steps in each summary."""
        from sql_databricks_bridge.core.extractor import ExtractionJob

        job = ExtractionJob(
            job_id="integ-004",
            country="bolivia",
            queries=["q1"],
        )
        mock_extractor = MagicMock()
        mock_extractor.create_job.return_value = job
        mock_extractor.execute_query.return_value = []
        MockExtractor.return_value = mock_extractor

        # Trigger
        test_client.post("/api/v1/trigger", json={
            "country": "bolivia",
            "stage": "calibracion",
        })

        # List events -- the in-flight job should appear
        response = test_client.get("/api/v1/events")
        assert response.status_code == 200
        data = response.json()

        # Find our job in the list
        our_jobs = [item for item in data["items"] if item["job_id"] == "integ-004"]
        assert len(our_jobs) == 1

        job_data = our_jobs[0]
        assert "steps" in job_data
        assert job_data["steps"] is not None
        assert len(job_data["steps"]) == 6
        assert "current_step" in job_data

    def test_step_structure_matches_frontend_contract(self):
        """Verify CalibrationStepResponse serializes to the exact shape the frontend expects."""
        from sql_databricks_bridge.api.routes.trigger import CalibrationStepResponse

        step = CalibrationStepResponse(
            name="sync_data",
            status="completed",
            started_at=datetime(2026, 2, 14, 12, 0, 0),
            completed_at=datetime(2026, 2, 14, 12, 5, 0),
            error=None,
        )
        data = step.model_dump(mode="json")

        # These are the exact fields the frontend TypeScript CalibrationStep expects
        assert set(data.keys()) == {"name", "status", "started_at", "completed_at", "error"}
        assert isinstance(data["name"], str)
        assert isinstance(data["status"], str)
        assert isinstance(data["started_at"], str)  # ISO string
        assert isinstance(data["completed_at"], str)  # ISO string
        assert data["error"] is None
