"""Comprehensive mock-based tests for all frontend-facing API endpoints.

Covers:
  GET  /api/v1/health/live
  GET  /api/v1/metadata/countries
  GET  /api/v1/metadata/stages
  GET  /api/v1/metadata/data-availability
  POST /api/v1/trigger
  GET  /api/v1/events
  GET  /api/v1/events/{job_id}
  GET  /api/v1/events/{job_id}/download

No SQL Server or Databricks connection required. All I/O is mocked.

Run with:
    PYTHONPATH=src pytest tests/unit/test_api_endpoints.py -v
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from sql_databricks_bridge.api.dependencies import get_current_azure_ad_user
from sql_databricks_bridge.auth.authorized_users import AuthorizedUser
from sql_databricks_bridge.api.routes.trigger import _trigger_jobs, _TriggerJobRecord
from sql_databricks_bridge.api.schemas import JobStatus, QueryResult
from sql_databricks_bridge.core.extractor import ExtractionJob


# ---------------------------------------------------------------------------
# Shared user fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def admin_user() -> AuthorizedUser:
    return AuthorizedUser(
        email="admin@test.com",
        name="Admin User",
        roles=["admin"],
        countries=["*"],
    )


@pytest.fixture
def operator_user() -> AuthorizedUser:
    return AuthorizedUser(
        email="operator@test.com",
        name="Operator User",
        roles=["operator"],
        countries=["bolivia", "chile"],
    )


@pytest.fixture
def viewer_user() -> AuthorizedUser:
    return AuthorizedUser(
        email="viewer@test.com",
        name="Viewer User",
        roles=["viewer"],
        countries=["bolivia"],
    )


# ---------------------------------------------------------------------------
# Shared TestClient factory
# ---------------------------------------------------------------------------


def _make_test_client(user: AuthorizedUser) -> TestClient:
    """Build a TestClient with mocked auth, no real Databricks/SQL connections.

    The lifespan is skipped by setting _event_poller=None so no connections are
    attempted at startup.
    """
    from sql_databricks_bridge.api.routes import trigger as trigger_module

    _trigger_jobs.clear()
    saved_launcher = trigger_module._calibration_launcher
    trigger_module._calibration_launcher = None

    with patch("sql_databricks_bridge.main._event_poller", None):
        from sql_databricks_bridge.main import app

        mock_db_client = MagicMock()
        mock_db_client.execute_sql.return_value = []
        mock_db_client.test_connection.return_value = True
        app.state.databricks_client = mock_db_client

        app.dependency_overrides[get_current_azure_ad_user] = lambda: user

        client = TestClient(app)
        yield client

        app.dependency_overrides.pop(get_current_azure_ad_user, None)
        _trigger_jobs.clear()
        trigger_module._calibration_launcher = saved_launcher


@pytest.fixture
def admin_client(admin_user):
    yield from _make_test_client(admin_user)


@pytest.fixture
def operator_client(operator_user):
    yield from _make_test_client(operator_user)


@pytest.fixture
def viewer_client(viewer_user):
    yield from _make_test_client(viewer_user)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_job_record(
    job_id: str,
    country: str = "bolivia",
    triggered_by: str = "admin@test.com",
    status: JobStatus = JobStatus.PENDING,
    queries: list[str] | None = None,
    results: list[QueryResult] | None = None,
    stage: str = "calibracion",
    tag: str = "",
) -> _TriggerJobRecord:
    job = ExtractionJob(
        job_id=job_id,
        country=country,
        queries=queries or ["q1"],
        status=status,
        results=results or [],
    )
    return _TriggerJobRecord(
        extractor=MagicMock(),
        job=job,
        triggered_by=triggered_by,
        stage=stage,
        tag=tag or f"{country}-{stage}-2026-02-18",
    )


# ===========================================================================
# 1. Health endpoints
# ===========================================================================


class TestHealthLive:
    """GET /api/v1/health/live"""

    def test_returns_200_ok(self, admin_client):
        response = admin_client.get("/api/v1/health/live")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_returns_200_without_auth(self):
        """Liveness probe does not require authentication."""
        with patch("sql_databricks_bridge.main._event_poller", None):
            from sql_databricks_bridge.main import app
            client = TestClient(app)
            response = client.get("/api/v1/health/live")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    def test_response_has_status_key(self, admin_client):
        data = admin_client.get("/api/v1/health/live").json()
        assert "status" in data


class TestHealthReady:
    """GET /api/v1/health/ready"""

    def test_returns_healthy_when_both_connected(self):
        """Readiness returns healthy when SQL Server and Databricks are reachable."""
        with patch("sql_databricks_bridge.main._event_poller", None):
            with patch("sql_databricks_bridge.api.routes.health.SQLServerClient") as mock_sql:
                with patch("sql_databricks_bridge.api.routes.health.DatabricksClient") as mock_db:
                    mock_sql.return_value.test_connection.return_value = True
                    mock_sql.return_value.close.return_value = None
                    mock_db.return_value.test_connection.return_value = True

                    from sql_databricks_bridge.main import app
                    client = TestClient(app)
                    response = client.get("/api/v1/health/ready")

        assert response.status_code == 200
        assert response.json()["status"] == "healthy"

    def test_returns_unhealthy_when_sql_down(self):
        with patch("sql_databricks_bridge.main._event_poller", None):
            with patch("sql_databricks_bridge.api.routes.health.SQLServerClient") as mock_sql:
                with patch("sql_databricks_bridge.api.routes.health.DatabricksClient") as mock_db:
                    mock_sql.return_value.test_connection.return_value = False
                    mock_sql.return_value.close.return_value = None
                    mock_db.return_value.test_connection.return_value = True

                    from sql_databricks_bridge.main import app
                    client = TestClient(app)
                    response = client.get("/api/v1/health/ready")

        data = response.json()
        assert data["status"] == "unhealthy"
        assert data["components"]["sql_server"]["status"] == "unhealthy"

    def test_response_includes_version_and_environment(self):
        with patch("sql_databricks_bridge.main._event_poller", None):
            with patch("sql_databricks_bridge.api.routes.health.SQLServerClient") as mock_sql:
                with patch("sql_databricks_bridge.api.routes.health.DatabricksClient") as mock_db:
                    mock_sql.return_value.test_connection.return_value = True
                    mock_sql.return_value.close.return_value = None
                    mock_db.return_value.test_connection.return_value = True

                    from sql_databricks_bridge.main import app
                    client = TestClient(app)
                    data = client.get("/api/v1/health/ready").json()

        assert "version" in data
        assert "environment" in data
        assert "components" in data


# ===========================================================================
# 2. Metadata - Countries
# ===========================================================================


class TestMetadataCountries:
    """GET /api/v1/metadata/countries"""

    def test_returns_200(self, admin_client):
        with patch("sql_databricks_bridge.api.routes.metadata.CountryAwareQueryLoader") as mock_loader:
            instance = mock_loader.return_value
            instance.list_all_entries.return_value = [("bolivia", "country"), ("chile", "country")]
            instance.list_queries.side_effect = lambda name: ["q1", "q2"] if name == "bolivia" else ["q3"]
            response = admin_client.get("/api/v1/metadata/countries")

        assert response.status_code == 200

    def test_returns_countries_list(self, admin_client):
        with patch("sql_databricks_bridge.api.routes.metadata.CountryAwareQueryLoader") as mock_loader:
            instance = mock_loader.return_value
            instance.list_all_entries.return_value = [("bolivia", "country"), ("chile", "country")]
            instance.list_queries.side_effect = lambda name: ["q1", "q2"] if name == "bolivia" else ["q3"]
            response = admin_client.get("/api/v1/metadata/countries")

        data = response.json()
        assert "countries" in data
        assert len(data["countries"]) == 2

    def test_country_entry_has_required_fields(self, admin_client):
        with patch("sql_databricks_bridge.api.routes.metadata.CountryAwareQueryLoader") as mock_loader:
            instance = mock_loader.return_value
            instance.list_all_entries.return_value = [("bolivia", "country")]
            instance.list_queries.return_value = ["q1", "q2"]
            response = admin_client.get("/api/v1/metadata/countries")

        country = response.json()["countries"][0]
        assert "code" in country
        assert "queries" in country
        assert "queries_count" in country
        assert "type" in country

    def test_queries_count_matches_queries_list(self, admin_client):
        with patch("sql_databricks_bridge.api.routes.metadata.CountryAwareQueryLoader") as mock_loader:
            instance = mock_loader.return_value
            instance.list_all_entries.return_value = [("bolivia", "country")]
            instance.list_queries.return_value = ["q1", "q2", "q3"]
            response = admin_client.get("/api/v1/metadata/countries")

        country = response.json()["countries"][0]
        assert country["queries_count"] == 3
        assert len(country["queries"]) == 3

    def test_empty_countries(self, admin_client):
        with patch("sql_databricks_bridge.api.routes.metadata.CountryAwareQueryLoader") as mock_loader:
            instance = mock_loader.return_value
            instance.list_all_entries.return_value = []
            response = admin_client.get("/api/v1/metadata/countries")

        data = response.json()
        assert data["countries"] == []

    def test_server_type_entry(self, admin_client):
        """Entries with type='server' are also returned."""
        with patch("sql_databricks_bridge.api.routes.metadata.CountryAwareQueryLoader") as mock_loader:
            instance = mock_loader.return_value
            instance.list_all_entries.return_value = [("prod-server", "server")]
            instance.list_queries.return_value = ["sp_query"]
            response = admin_client.get("/api/v1/metadata/countries")

        entry = response.json()["countries"][0]
        assert entry["type"] == "server"


# ===========================================================================
# 3. Metadata - Stages
# ===========================================================================


class TestMetadataStages:
    """GET /api/v1/metadata/stages"""

    def test_returns_200(self, admin_client):
        with patch("sql_databricks_bridge.api.routes.metadata.load_stages") as mock_stages:
            mock_stages.return_value = [
                {"code": "calibracion", "name": "Calibración"},
                {"code": "mtr", "name": "MTR"},
            ]
            response = admin_client.get("/api/v1/metadata/stages")

        assert response.status_code == 200

    def test_returns_stages_list(self, admin_client):
        with patch("sql_databricks_bridge.api.routes.metadata.load_stages") as mock_stages:
            mock_stages.return_value = [
                {"code": "calibracion", "name": "Calibración"},
                {"code": "mtr", "name": "MTR"},
            ]
            response = admin_client.get("/api/v1/metadata/stages")

        data = response.json()
        assert "stages" in data
        assert len(data["stages"]) == 2

    def test_stage_entry_has_code_and_name(self, admin_client):
        with patch("sql_databricks_bridge.api.routes.metadata.load_stages") as mock_stages:
            mock_stages.return_value = [{"code": "calibracion", "name": "Calibración"}]
            response = admin_client.get("/api/v1/metadata/stages")

        stage = response.json()["stages"][0]
        assert stage["code"] == "calibracion"
        assert stage["name"] == "Calibración"

    def test_empty_stages(self, admin_client):
        with patch("sql_databricks_bridge.api.routes.metadata.load_stages") as mock_stages:
            mock_stages.return_value = []
            response = admin_client.get("/api/v1/metadata/stages")

        assert response.json()["stages"] == []


# ===========================================================================
# 4. Metadata - Data Availability
# ===========================================================================


class TestMetadataDataAvailability:
    """GET /api/v1/metadata/data-availability?period=YYYYMM"""

    def test_returns_200_with_valid_period(self, admin_client):
        with patch("sql_databricks_bridge.api.routes.metadata.SQLServerClient") as mock_sql:
            mock_sql.return_value.execute_query.return_value = []
            response = admin_client.get(
                "/api/v1/metadata/data-availability", params={"period": "202602"}
            )

        assert response.status_code == 200

    def test_response_includes_period(self, admin_client):
        with patch("sql_databricks_bridge.api.routes.metadata.SQLServerClient") as mock_sql:
            mock_sql.return_value.execute_query.return_value = []
            response = admin_client.get(
                "/api/v1/metadata/data-availability", params={"period": "202601"}
            )

        data = response.json()
        assert data["period"] == "202601"
        assert "countries" in data

    def test_invalid_period_format_rejected(self, admin_client):
        response = admin_client.get(
            "/api/v1/metadata/data-availability", params={"period": "2026-02"}
        )
        assert response.status_code == 422

    def test_missing_period_rejected(self, admin_client):
        response = admin_client.get("/api/v1/metadata/data-availability")
        assert response.status_code == 422

    def test_no_countries_when_no_queries_dir(self, admin_client):
        """When the countries directory does not exist, returns empty dict."""
        with patch("sql_databricks_bridge.api.routes.metadata.Path") as mock_path_cls:
            # Make countries_path.exists() return False
            mock_queries_base = MagicMock()
            mock_countries_path = MagicMock()
            mock_countries_path.exists.return_value = False
            mock_queries_base.__truediv__ = lambda self, other: mock_countries_path
            mock_path_cls.return_value = mock_queries_base

            response = admin_client.get(
                "/api/v1/metadata/data-availability", params={"period": "202602"}
            )

        assert response.status_code == 200
        assert response.json()["countries"] == {}


# ===========================================================================
# 5. Trigger endpoint
# ===========================================================================


class TestTriggerEndpoint:
    """POST /api/v1/trigger"""

    @patch("sql_databricks_bridge.api.routes.trigger.update_job_status")
    @patch("sql_databricks_bridge.api.routes.trigger.insert_job")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_trigger_returns_201(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockInsertJob,
        MockUpdateStatus,
        admin_client,
    ):
        mock_extractor = MagicMock()
        mock_extractor.execute_query.return_value = iter([])
        mock_job = _make_mock_extraction_job("trig-001")
        mock_extractor.create_job.return_value = mock_job
        MockExtractor.return_value = mock_extractor

        response = admin_client.post(
            "/api/v1/trigger",
            json={"country": "bolivia", "stage": "calibracion"},
        )

        assert response.status_code == 201

    @patch("sql_databricks_bridge.api.routes.trigger.update_job_status")
    @patch("sql_databricks_bridge.api.routes.trigger.insert_job")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_trigger_response_has_job_id(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockInsertJob,
        MockUpdateStatus,
        admin_client,
    ):
        mock_extractor = MagicMock()
        mock_extractor.execute_query.return_value = iter([])
        mock_job = _make_mock_extraction_job("trig-002")
        mock_extractor.create_job.return_value = mock_job
        MockExtractor.return_value = mock_extractor

        data = admin_client.post(
            "/api/v1/trigger",
            json={"country": "bolivia", "stage": "calibracion"},
        ).json()

        assert "job_id" in data
        assert data["country"] == "bolivia"
        assert data["stage"] == "calibracion"
        assert data["status"] == "pending"

    @patch("sql_databricks_bridge.api.routes.trigger.update_job_status")
    @patch("sql_databricks_bridge.api.routes.trigger.insert_job")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_trigger_with_period(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockInsertJob,
        MockUpdateStatus,
        admin_client,
    ):
        mock_extractor = MagicMock()
        mock_extractor.execute_query.return_value = iter([])
        mock_job = _make_mock_extraction_job("trig-003")
        mock_extractor.create_job.return_value = mock_job
        MockExtractor.return_value = mock_extractor

        data = admin_client.post(
            "/api/v1/trigger",
            json={"country": "bolivia", "stage": "calibracion", "period": "202602"},
        ).json()

        assert data["period"] == "202602"

    @patch("sql_databricks_bridge.api.routes.trigger.update_job_status")
    @patch("sql_databricks_bridge.api.routes.trigger.insert_job")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_trigger_records_triggered_by(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockInsertJob,
        MockUpdateStatus,
        admin_client,
    ):
        mock_extractor = MagicMock()
        mock_extractor.execute_query.return_value = iter([])
        mock_job = _make_mock_extraction_job("trig-004")
        mock_extractor.create_job.return_value = mock_job
        MockExtractor.return_value = mock_extractor

        data = admin_client.post(
            "/api/v1/trigger",
            json={"country": "bolivia", "stage": "calibracion"},
        ).json()

        assert data["triggered_by"] == "admin@test.com"

    def test_trigger_forbidden_for_viewer(self, viewer_client):
        """Viewer role cannot trigger syncs."""
        response = viewer_client.post(
            "/api/v1/trigger",
            json={"country": "bolivia", "stage": "calibracion"},
        )
        assert response.status_code == 403

    @patch("sql_databricks_bridge.api.routes.trigger.update_job_status")
    @patch("sql_databricks_bridge.api.routes.trigger.insert_job")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_trigger_forbidden_for_wrong_country(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockInsertJob,
        MockUpdateStatus,
        operator_client,
    ):
        """Operator cannot trigger for a country not in their allowlist."""
        mock_extractor = MagicMock()
        mock_extractor.execute_query.return_value = iter([])
        mock_job = _make_mock_extraction_job("trig-005")
        mock_extractor.create_job.return_value = mock_job
        MockExtractor.return_value = mock_extractor

        response = operator_client.post(
            "/api/v1/trigger",
            json={"country": "brasil", "stage": "calibracion"},
        )
        assert response.status_code == 403

    @patch("sql_databricks_bridge.api.routes.trigger.update_job_status")
    @patch("sql_databricks_bridge.api.routes.trigger.insert_job")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_trigger_country_not_found(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockInsertJob,
        MockUpdateStatus,
        admin_client,
    ):
        """When queries directory for a country does not exist, returns 404."""
        MockExtractor.return_value.create_job.side_effect = FileNotFoundError("queries/countries/nonexistent not found")

        response = admin_client.post(
            "/api/v1/trigger",
            json={"country": "nonexistent", "stage": "calibracion"},
        )
        assert response.status_code == 404

    @patch("sql_databricks_bridge.api.routes.trigger.update_job_status")
    @patch("sql_databricks_bridge.api.routes.trigger.insert_job")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_trigger_response_includes_queries_list(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockInsertJob,
        MockUpdateStatus,
        admin_client,
    ):
        mock_extractor = MagicMock()
        mock_extractor.execute_query.return_value = iter([])
        mock_job = _make_mock_extraction_job("trig-006", queries=["q1", "q2", "q3"])
        mock_extractor.create_job.return_value = mock_job
        MockExtractor.return_value = mock_extractor

        data = admin_client.post(
            "/api/v1/trigger",
            json={"country": "bolivia", "stage": "calibracion"},
        ).json()

        assert data["queries_count"] == 3
        assert len(data["queries"]) == 3


def _make_mock_extraction_job(
    job_id: str,
    country: str = "bolivia",
    queries: list[str] | None = None,
) -> MagicMock:
    """Build a MagicMock that looks like an ExtractionJob for trigger tests."""
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


# ===========================================================================
# 6. Events list endpoint
# ===========================================================================


class TestEventsListEndpoint:
    """GET /api/v1/events"""

    def test_empty_returns_200(self, admin_client):
        response = admin_client.get("/api/v1/events")
        assert response.status_code == 200

    def test_empty_response_structure(self, admin_client):
        data = admin_client.get("/api/v1/events").json()
        assert "items" in data
        assert "total" in data
        assert "limit" in data
        assert "offset" in data
        assert data["items"] == []
        assert data["total"] == 0

    def test_admin_sees_all_jobs(self, admin_client):
        _trigger_jobs["j1"] = _make_job_record("j1", "bolivia", "admin@test.com")
        _trigger_jobs["j2"] = _make_job_record("j2", "chile", "operator@test.com")

        data = admin_client.get("/api/v1/events").json()
        assert data["total"] == 2

    def test_operator_sees_only_own_jobs(self, operator_client):
        _trigger_jobs["j-op"] = _make_job_record("j-op", "bolivia", "operator@test.com")
        _trigger_jobs["j-other"] = _make_job_record("j-other", "chile", "admin@test.com")

        data = operator_client.get("/api/v1/events").json()
        assert data["total"] == 1
        assert data["items"][0]["job_id"] == "j-op"

    def test_filter_by_country(self, admin_client):
        _trigger_jobs["j-bo"] = _make_job_record("j-bo", "bolivia", "admin@test.com")
        _trigger_jobs["j-cl"] = _make_job_record("j-cl", "chile", "admin@test.com")

        data = admin_client.get("/api/v1/events", params={"country": "bolivia"}).json()
        assert data["total"] == 1
        assert data["items"][0]["country"] == "bolivia"

    def test_filter_by_status(self, admin_client):
        _trigger_jobs["j-done"] = _make_job_record("j-done", status=JobStatus.COMPLETED)
        _trigger_jobs["j-run"] = _make_job_record("j-run", status=JobStatus.RUNNING)

        data = admin_client.get("/api/v1/events", params={"status": "completed"}).json()
        assert data["total"] == 1
        assert data["items"][0]["status"] == "completed"

    def test_filter_by_stage(self, admin_client):
        _trigger_jobs["j-cal"] = _make_job_record("j-cal", stage="calibracion")
        _trigger_jobs["j-mtr"] = _make_job_record("j-mtr", stage="mtr")

        data = admin_client.get("/api/v1/events", params={"stage": "calibracion"}).json()
        assert data["total"] == 1

    def test_pagination_limit_in_response_metadata(self, admin_client):
        """The limit param is echoed in the response metadata.

        Note: pagination (slicing items) applies at SQLite/Delta layer.
        The in-memory path returns all matching items regardless of limit.
        """
        for i in range(5):
            _trigger_jobs[f"j-{i}"] = _make_job_record(f"j-{i}")

        data = admin_client.get("/api/v1/events", params={"limit": 2}).json()
        assert data["total"] == 5
        assert data["limit"] == 2

    def test_pagination_offset_in_response_metadata(self, admin_client):
        """The offset param is echoed in the response metadata."""
        for i in range(5):
            _trigger_jobs[f"j-{i}"] = _make_job_record(f"j-{i}")

        data = admin_client.get("/api/v1/events", params={"limit": 2, "offset": 3}).json()
        assert data["total"] == 5
        assert data["offset"] == 3

    def test_event_item_fields(self, admin_client):
        """Each item has the required frontend-facing fields."""
        _trigger_jobs["j-fields"] = _make_job_record("j-fields", "bolivia")

        data = admin_client.get("/api/v1/events").json()
        item = data["items"][0]
        for field in ["job_id", "status", "country", "stage", "tag", "queries_total",
                      "queries_completed", "queries_failed", "created_at", "triggered_by"]:
            assert field in item, f"Missing field: {field}"


# ===========================================================================
# 7. Event detail endpoint
# ===========================================================================


class TestEventDetailEndpoint:
    """GET /api/v1/events/{job_id}"""

    def test_returns_200_for_existing_job(self, admin_client):
        _trigger_jobs["detail-1"] = _make_job_record("detail-1", "bolivia")
        response = admin_client.get("/api/v1/events/detail-1")
        assert response.status_code == 200

    def test_returns_404_for_missing_job(self, admin_client):
        response = admin_client.get("/api/v1/events/nonexistent")
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "not_found"

    def test_admin_can_see_any_job(self, admin_client):
        _trigger_jobs["detail-2"] = _make_job_record("detail-2", triggered_by="operator@test.com")
        response = admin_client.get("/api/v1/events/detail-2")
        assert response.status_code == 200

    def test_operator_can_see_own_job(self, operator_client):
        _trigger_jobs["detail-3"] = _make_job_record("detail-3", triggered_by="operator@test.com")
        response = operator_client.get("/api/v1/events/detail-3")
        assert response.status_code == 200

    def test_operator_cannot_see_other_job(self, operator_client):
        _trigger_jobs["detail-4"] = _make_job_record("detail-4", triggered_by="admin@test.com")
        response = operator_client.get("/api/v1/events/detail-4")
        assert response.status_code == 404

    def test_detail_includes_results(self, admin_client):
        results = [
            QueryResult(query_name="q1", status=JobStatus.COMPLETED, rows_extracted=100, duration_seconds=1.0),
            QueryResult(query_name="q2", status=JobStatus.FAILED, error="Timeout", duration_seconds=30.0),
        ]
        _trigger_jobs["detail-5"] = _make_job_record(
            "detail-5", queries=["q1", "q2"], results=results, status=JobStatus.COMPLETED
        )
        data = admin_client.get("/api/v1/events/detail-5").json()
        assert len(data["results"]) == 2
        assert data["results"][0]["query_name"] == "q1"
        assert data["results"][0]["rows_extracted"] == 100
        assert data["results"][1]["status"] == "failed"

    def test_detail_includes_error_for_failed_job(self, admin_client):
        job = ExtractionJob(
            job_id="detail-6",
            country="chile",
            queries=["q1"],
            status=JobStatus.FAILED,
            error="SQL Server connection refused",
        )
        _trigger_jobs["detail-6"] = _TriggerJobRecord(
            extractor=MagicMock(), job=job, triggered_by="admin@test.com"
        )
        data = admin_client.get("/api/v1/events/detail-6").json()
        assert data["status"] == "failed"
        assert data["error"] == "SQL Server connection refused"

    def test_detail_has_all_required_fields(self, admin_client):
        _trigger_jobs["detail-7"] = _make_job_record("detail-7", "bolivia")
        data = admin_client.get("/api/v1/events/detail-7").json()
        for field in ["job_id", "status", "country", "stage", "tag", "queries_total",
                      "queries_completed", "queries_failed", "created_at", "triggered_by",
                      "results"]:
            assert field in data, f"Missing field: {field}"

    def test_detail_job_id_matches(self, admin_client):
        _trigger_jobs["detail-8"] = _make_job_record("detail-8", "bolivia")
        data = admin_client.get("/api/v1/events/detail-8").json()
        assert data["job_id"] == "detail-8"


# ===========================================================================
# 8. CSV Download endpoint
# ===========================================================================


class TestDownloadEndpoint:
    """GET /api/v1/events/{job_id}/download"""

    _FAKE_TABLE = "`main`.`bolivia`.`j_atoscompra_new`"
    _FAKE_ROWS = [{"id": 1, "periodo": "202601", "value": 42.5}]

    def _setup_completed_job(self, job_id, country="bolivia", rows_extracted=50):
        """Create a completed job record with a table_name and mock Databricks data."""
        results = [
            QueryResult(
                query_name="q1",
                status=JobStatus.COMPLETED,
                rows_extracted=rows_extracted,
                table_name=self._FAKE_TABLE,
                duration_seconds=1.0,
            ),
        ]
        _trigger_jobs[job_id] = _make_job_record(
            job_id, country,
            status=JobStatus.COMPLETED,
            results=results,
        )
        from sql_databricks_bridge.core.calibration_tracker import calibration_tracker
        calibration_tracker.create(job_id, country=country)
        calibration_tracker.start_step(job_id, "sync_data")
        calibration_tracker.complete_step(job_id, "sync_data")

    def _cleanup(self, job_id):
        from sql_databricks_bridge.core.calibration_tracker import calibration_tracker
        calibration_tracker.delete(job_id)

    def test_download_completed_job_returns_200(self, admin_client):
        self._setup_completed_job("dl-1")
        # Mock Databricks to return actual rows
        admin_client.app.state.databricks_client.execute_sql.return_value = self._FAKE_ROWS
        response = admin_client.get("/api/v1/events/dl-1/download")
        assert response.status_code == 200
        self._cleanup("dl-1")

    def test_download_returns_csv_content_type(self, admin_client):
        self._setup_completed_job("dl-2")
        admin_client.app.state.databricks_client.execute_sql.return_value = self._FAKE_ROWS
        response = admin_client.get("/api/v1/events/dl-2/download")
        assert "text/csv" in response.headers.get("content-type", "")
        self._cleanup("dl-2")

    def test_download_not_found(self, admin_client):
        response = admin_client.get("/api/v1/events/nonexistent/download")
        assert response.status_code == 404

    def test_download_pending_job_returns_409(self, admin_client):
        _trigger_jobs["dl-3"] = _make_job_record("dl-3", status=JobStatus.PENDING)
        response = admin_client.get("/api/v1/events/dl-3/download")
        assert response.status_code == 409

    def test_download_running_job_returns_409(self, admin_client):
        _trigger_jobs["dl-4"] = _make_job_record("dl-4", status=JobStatus.RUNNING)
        response = admin_client.get("/api/v1/events/dl-4/download")
        assert response.status_code == 409

    def test_download_csv_contains_real_data(self, admin_client):
        """CSV body contains actual table data (column headers + rows)."""
        self._setup_completed_job("dl-5", country="chile", rows_extracted=100)
        admin_client.app.state.databricks_client.execute_sql.return_value = [
            {"id": 1, "periodo": "202601", "value": 99.9},
            {"id": 2, "periodo": "202602", "value": 50.0},
        ]
        response = admin_client.get("/api/v1/events/dl-5/download")
        content = response.text
        assert "id,periodo,value" in content
        assert "202601" in content
        assert "99.9" in content
        self._cleanup("dl-5")

    def test_download_filename_header(self, admin_client):
        self._setup_completed_job("dl-6")
        admin_client.app.state.databricks_client.execute_sql.return_value = self._FAKE_ROWS
        response = admin_client.get("/api/v1/events/dl-6/download")
        disposition = response.headers.get("content-disposition", "")
        assert "attachment" in disposition
        assert ".csv" in disposition
        assert "j_atoscompra_new" in disposition
        self._cleanup("dl-6")

    def test_download_no_tables_returns_404(self, admin_client):
        """Job completed but no result tables available."""
        results = [
            QueryResult(
                query_name="q1",
                status=JobStatus.COMPLETED,
                rows_extracted=0,
                duration_seconds=1.0,
            ),
        ]
        _trigger_jobs["dl-7"] = _make_job_record(
            "dl-7", "bolivia",
            status=JobStatus.COMPLETED,
            results=results,
        )
        from sql_databricks_bridge.core.calibration_tracker import calibration_tracker
        calibration_tracker.create("dl-7", country="bolivia")
        calibration_tracker.start_step("dl-7", "sync_data")
        calibration_tracker.complete_step("dl-7", "sync_data")

        response = admin_client.get("/api/v1/events/dl-7/download")
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "no_tables"
        calibration_tracker.delete("dl-7")

    def test_download_specific_table(self, admin_client):
        """Can request a specific table via ?table= query param."""
        self._setup_completed_job("dl-8")
        admin_client.app.state.databricks_client.execute_sql.return_value = self._FAKE_ROWS
        response = admin_client.get(
            "/api/v1/events/dl-8/download",
            params={"table": self._FAKE_TABLE},
        )
        assert response.status_code == 200
        self._cleanup("dl-8")


# ===========================================================================
# 9. Authentication enforcement
# ===========================================================================


class TestAuthEnforcement:
    """Verifies that auth dependency controls access to protected endpoints."""

    def test_trigger_requires_auth_dependency(self, admin_client):
        """Trigger endpoint works with a valid authenticated user."""
        # This test verifies the auth dependency is wired correctly by confirming
        # that a properly authenticated user can reach the endpoint.
        # The actual 401 behavior requires real Azure AD config (integration test).
        with (
            patch("sql_databricks_bridge.api.routes.trigger.Extractor") as MockExtractor,
            patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient"),
            patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter"),
            patch("sql_databricks_bridge.api.routes.trigger.insert_job"),
            patch("sql_databricks_bridge.api.routes.trigger.update_job_status"),
        ):
            mock_extractor = MagicMock()
            mock_extractor.execute_query.return_value = iter([])
            mock_job = _make_mock_extraction_job("auth-001")
            mock_extractor.create_job.return_value = mock_job
            MockExtractor.return_value = mock_extractor

            response = admin_client.post(
                "/api/v1/trigger",
                json={"country": "bolivia", "stage": "calibracion"},
            )
        assert response.status_code == 201

    def test_events_accessible_with_auth(self, admin_client):
        """Events endpoint is accessible to an authenticated user."""
        response = admin_client.get("/api/v1/events")
        assert response.status_code == 200

    def test_health_live_does_not_require_auth(self):
        """Liveness probe is always accessible, regardless of auth."""
        with patch("sql_databricks_bridge.main._event_poller", None):
            from sql_databricks_bridge.main import app
            # Temporarily clear overrides for this isolated test
            saved_overrides = dict(app.dependency_overrides)
            app.dependency_overrides.clear()
            try:
                client = TestClient(app)
                response = client.get("/api/v1/health/live")
            finally:
                app.dependency_overrides.update(saved_overrides)
        assert response.status_code == 200

    def test_viewer_cannot_trigger_sync(self, viewer_client):
        """Viewer role is blocked from triggering syncs (403 Forbidden)."""
        response = viewer_client.post(
            "/api/v1/trigger",
            json={"country": "bolivia", "stage": "calibracion"},
        )
        assert response.status_code == 403

    def test_operator_blocked_from_unauthorized_country(self, operator_client):
        """Operator cannot trigger for countries outside their allowlist (403)."""
        response = operator_client.post(
            "/api/v1/trigger",
            json={"country": "colombia", "stage": "calibracion"},
        )
        assert response.status_code == 403


# ===========================================================================
# 10. GAP-7: Steps persistence to SQLite
# ===========================================================================


class TestGAP7StepsPersistence:
    """Tests for GAP-7: calibration steps are persisted to SQLite and
    survive a cold read (in-memory calibration_tracker cleared).

    Uses a real SQLite temp database so that local_store calls are live.
    The trigger endpoint writes steps via _persist_steps() when db_path
    is set on app.state.
    """

    @patch("sql_databricks_bridge.api.routes.trigger.update_job_status")
    @patch("sql_databricks_bridge.api.routes.trigger.insert_job")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_steps_persisted_to_sqlite_after_trigger(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockInsertJob,
        MockUpdateStatus,
        admin_user,
        tmp_path,
    ):
        """After POST /trigger, calibration steps are written to SQLite steps_json."""
        import tempfile
        from sql_databricks_bridge.db import local_store
        from sql_databricks_bridge.api.routes import trigger as trigger_module
        from sql_databricks_bridge.core.calibration_tracker import calibration_tracker

        db_path = str(tmp_path / "jobs.db")
        local_store.init_db(db_path)

        _trigger_jobs.clear()
        saved_launcher = trigger_module._calibration_launcher
        trigger_module._calibration_launcher = None

        mock_extractor = MagicMock()
        mock_extractor.execute_query.return_value = iter([])
        mock_job = _make_mock_extraction_job("gap7-001")
        mock_extractor.create_job.return_value = mock_job
        MockExtractor.return_value = mock_extractor

        with patch("sql_databricks_bridge.main._event_poller", None):
            from sql_databricks_bridge.main import app
            app.state.sqlite_db_path = db_path

            mock_db_client = MagicMock()
            mock_db_client.execute_sql.return_value = []
            app.state.databricks_client = mock_db_client

            app.dependency_overrides[get_current_azure_ad_user] = lambda: admin_user
            client = TestClient(app)

            response = client.post(
                "/api/v1/trigger",
                json={"country": "bolivia", "stage": "calibracion"},
            )

            app.dependency_overrides.pop(get_current_azure_ad_user, None)

        assert response.status_code == 201
        job_id = response.json()["job_id"]

        # Verify steps were persisted to SQLite
        row = local_store.get_job(db_path, job_id)
        assert row is not None, "Job should be in SQLite"
        assert row.get("steps_json") is not None, "steps_json should be populated"
        steps = row["steps_json"]
        assert isinstance(steps, list), "steps_json should deserialize to a list"
        assert len(steps) > 0, "At least one step should be persisted"

        # Verify step names are present
        step_names = [s["name"] for s in steps]
        assert "sync_data" in step_names, "sync_data step should be persisted"

        # Cleanup
        _trigger_jobs.clear()
        calibration_tracker.delete(job_id)
        trigger_module._calibration_launcher = saved_launcher

    @patch("sql_databricks_bridge.api.routes.trigger.update_job_status")
    @patch("sql_databricks_bridge.api.routes.trigger.insert_job")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_steps_survive_cold_read_after_tracker_cleared(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockInsertJob,
        MockUpdateStatus,
        admin_user,
        tmp_path,
    ):
        """Steps come from SQLite when calibration_tracker is cleared (server restart scenario)."""
        from sql_databricks_bridge.db import local_store
        from sql_databricks_bridge.api.routes import trigger as trigger_module
        from sql_databricks_bridge.core.calibration_tracker import calibration_tracker

        db_path = str(tmp_path / "jobs.db")
        local_store.init_db(db_path)

        _trigger_jobs.clear()
        saved_launcher = trigger_module._calibration_launcher
        trigger_module._calibration_launcher = None

        mock_extractor = MagicMock()
        mock_extractor.execute_query.return_value = iter([])
        mock_job = _make_mock_extraction_job("gap7-002")
        mock_extractor.create_job.return_value = mock_job
        MockExtractor.return_value = mock_extractor

        with patch("sql_databricks_bridge.main._event_poller", None):
            from sql_databricks_bridge.main import app
            app.state.sqlite_db_path = db_path

            mock_db_client = MagicMock()
            mock_db_client.execute_sql.return_value = []
            app.state.databricks_client = mock_db_client

            app.dependency_overrides[get_current_azure_ad_user] = lambda: admin_user
            client = TestClient(app)

            # Step 1: Trigger the job (steps go to in-memory tracker + SQLite)
            response = client.post(
                "/api/v1/trigger",
                json={"country": "bolivia", "stage": "calibracion"},
            )
            assert response.status_code == 201
            job_id = response.json()["job_id"]

            # Step 2: Simulate server restart — clear in-memory state
            _trigger_jobs.clear()
            calibration_tracker.delete(job_id)

            # Step 3: Cold read — GET /events/{job_id} should recover steps from SQLite
            response_detail = client.get(f"/api/v1/events/{job_id}")

            app.dependency_overrides.pop(get_current_azure_ad_user, None)

        assert response_detail.status_code == 200, (
            f"Expected 200, got {response_detail.status_code}: {response_detail.text}"
        )
        data = response_detail.json()
        assert data["steps"] is not None, "Steps should be recovered from SQLite on cold read"
        assert len(data["steps"]) > 0, "At least one step should be returned"

        step_names = [s["name"] for s in data["steps"]]
        assert "sync_data" in step_names, "sync_data step should be recovered"

        # Cleanup
        _trigger_jobs.clear()
        calibration_tracker.delete(job_id)
        trigger_module._calibration_launcher = saved_launcher

    def test_update_steps_writes_and_reads_correctly(self, tmp_path):
        """Unit test for local_store.update_steps() — correct serialization roundtrip."""
        from sql_databricks_bridge.db import local_store
        from datetime import datetime

        db_path = str(tmp_path / "jobs.db")
        local_store.init_db(db_path)

        # Insert a job first
        local_store.insert_job(
            db_path,
            job_id="gap7-steps-001",
            country="chile",
            stage="calibracion",
            tag="chile-calibracion-2026-02-18",
            queries=["q1"],
            triggered_by="admin@test.com",
            created_at="2026-02-18T10:00:00",
        )

        # Write calibration steps
        steps = [
            {"name": "sync_data", "status": "completed", "started_at": "2026-02-18T10:00:00", "completed_at": "2026-02-18T10:01:00", "error": None},
            {"name": "copy_to_calibration", "status": "running", "started_at": "2026-02-18T10:01:00", "completed_at": None, "error": None},
            {"name": "calibration", "status": "pending", "started_at": None, "completed_at": None, "error": None},
        ]
        local_store.update_steps(db_path, "gap7-steps-001", steps)

        # Read back and verify
        row = local_store.get_job(db_path, "gap7-steps-001")
        assert row is not None
        persisted = row["steps_json"]
        assert isinstance(persisted, list)
        assert len(persisted) == 3
        assert persisted[0]["name"] == "sync_data"
        assert persisted[0]["status"] == "completed"
        assert persisted[1]["name"] == "copy_to_calibration"
        assert persisted[1]["status"] == "running"
        assert persisted[2]["name"] == "calibration"
        assert persisted[2]["status"] == "pending"
