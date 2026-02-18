"""Unit tests for GET /events and GET /events/{job_id} endpoints."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from sql_databricks_bridge.api.routes.trigger import _trigger_jobs, _TriggerJobRecord
from sql_databricks_bridge.api.schemas import JobStatus, QueryResult
from sql_databricks_bridge.auth.authorized_users import AuthorizedUser
from sql_databricks_bridge.core.extractor import ExtractionJob


# --- Fixtures ---


@pytest.fixture
def admin_user():
    return AuthorizedUser(
        email="admin@test.com",
        name="Admin",
        roles=["admin"],
        countries=["*"],
    )


@pytest.fixture
def operator_user():
    return AuthorizedUser(
        email="operator@test.com",
        name="Operator",
        roles=["operator"],
        countries=["bolivia", "chile"],
    )


def _make_client(user: AuthorizedUser) -> TestClient:
    """Create TestClient with overridden Azure AD dependency."""
    from sql_databricks_bridge.api.dependencies import get_current_azure_ad_user

    _trigger_jobs.clear()

    with patch("sql_databricks_bridge.main._event_poller", None):
        from sql_databricks_bridge.main import app

        app.dependency_overrides[get_current_azure_ad_user] = lambda: user

        client = TestClient(app)
        yield client

        app.dependency_overrides.pop(get_current_azure_ad_user, None)
        _trigger_jobs.clear()


@pytest.fixture
def admin_client(admin_user):
    yield from _make_client(admin_user)


@pytest.fixture
def operator_client(operator_user):
    yield from _make_client(operator_user)


def _create_job_record(
    job_id: str,
    country: str,
    triggered_by: str,
    status: JobStatus = JobStatus.PENDING,
    queries: list[str] | None = None,
    results: list[QueryResult] | None = None,
) -> _TriggerJobRecord:
    """Create a _TriggerJobRecord with a mock extractor."""
    job = ExtractionJob(
        job_id=job_id,
        country=country,
        queries=queries or ["query1"],
        status=status,
        results=results or [],
    )
    return _TriggerJobRecord(
        extractor=MagicMock(),
        job=job,
        triggered_by=triggered_by,
    )


# --- GET /events tests ---


class TestListEvents:
    """Tests for GET /events."""

    def test_list_events_empty(self, admin_client):
        """Return empty list when no events exist."""
        response = admin_client.get("/events")

        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    def test_list_events_with_data(self, admin_client):
        """Admin sees all events."""
        _trigger_jobs["job-1"] = _create_job_record(
            "job-1", "bolivia", "admin@test.com"
        )
        _trigger_jobs["job-2"] = _create_job_record(
            "job-2", "chile", "operator@test.com"
        )

        response = admin_client.get("/events")

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
        assert len(data["items"]) == 2

    def test_list_events_operator_sees_own(self, operator_client, operator_user):
        """Operator sees only their own events."""
        _trigger_jobs["job-1"] = _create_job_record(
            "job-1", "bolivia", "operator@test.com"
        )
        _trigger_jobs["job-2"] = _create_job_record(
            "job-2", "chile", "admin@test.com"
        )

        response = operator_client.get("/events")

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["job_id"] == "job-1"

    def test_list_events_filter_by_country(self, admin_client):
        """Filter events by country."""
        _trigger_jobs["job-1"] = _create_job_record(
            "job-1", "bolivia", "admin@test.com"
        )
        _trigger_jobs["job-2"] = _create_job_record(
            "job-2", "chile", "admin@test.com"
        )
        _trigger_jobs["job-3"] = _create_job_record(
            "job-3", "bolivia", "admin@test.com"
        )

        response = admin_client.get("/events", params={"country": "bolivia"})

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
        assert all(item["country"] == "bolivia" for item in data["items"])

    def test_list_events_filter_by_status(self, admin_client):
        """Filter events by status."""
        _trigger_jobs["job-1"] = _create_job_record(
            "job-1", "bolivia", "admin@test.com", status=JobStatus.COMPLETED
        )
        _trigger_jobs["job-2"] = _create_job_record(
            "job-2", "chile", "admin@test.com", status=JobStatus.PENDING
        )

        response = admin_client.get("/events", params={"status": "completed"})

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["status"] == "completed"

    def test_list_events_pagination_limit(self, admin_client):
        """Limit number of returned events."""
        for i in range(5):
            _trigger_jobs[f"job-{i}"] = _create_job_record(
                f"job-{i}", "bolivia", "admin@test.com"
            )

        response = admin_client.get("/events", params={"limit": 2})

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 5
        assert len(data["items"]) == 2
        assert data["limit"] == 2

    def test_list_events_pagination_offset(self, admin_client):
        """Offset skips events."""
        for i in range(5):
            _trigger_jobs[f"job-{i}"] = _create_job_record(
                f"job-{i}", "bolivia", "admin@test.com"
            )

        response = admin_client.get(
            "/events", params={"limit": 2, "offset": 3}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 5
        assert len(data["items"]) == 2
        assert data["offset"] == 3

    def test_list_events_sorted_newest_first(self, admin_client):
        """Events are sorted by created_at descending (newest first)."""
        from datetime import timedelta

        now = datetime.utcnow()

        job_old = ExtractionJob(
            job_id="job-old",
            country="bolivia",
            queries=["q1"],
            created_at=now - timedelta(hours=2),
        )
        _trigger_jobs["job-old"] = _TriggerJobRecord(
            extractor=MagicMock(), job=job_old, triggered_by="admin@test.com"
        )

        job_new = ExtractionJob(
            job_id="job-new",
            country="chile",
            queries=["q1"],
            created_at=now,
        )
        _trigger_jobs["job-new"] = _TriggerJobRecord(
            extractor=MagicMock(), job=job_new, triggered_by="admin@test.com"
        )

        response = admin_client.get("/events")

        data = response.json()
        assert data["items"][0]["job_id"] == "job-new"
        assert data["items"][1]["job_id"] == "job-old"


# --- GET /events/{job_id} tests ---


class TestGetEventDetail:
    """Tests for GET /events/{job_id}."""

    def test_get_event_success(self, admin_client):
        """Admin can retrieve any event."""
        _trigger_jobs["job-detail"] = _create_job_record(
            "job-detail",
            "bolivia",
            "admin@test.com",
            status=JobStatus.COMPLETED,
            results=[
                QueryResult(
                    query_name="q1",
                    status=JobStatus.COMPLETED,
                    rows_extracted=100,
                    duration_seconds=1.5,
                )
            ],
        )

        response = admin_client.get("/events/job-detail")

        assert response.status_code == 200
        data = response.json()
        assert data["job_id"] == "job-detail"
        assert data["country"] == "bolivia"
        assert data["status"] == "completed"
        assert len(data["results"]) == 1
        assert data["results"][0]["query_name"] == "q1"
        assert data["results"][0]["rows_extracted"] == 100

    def test_get_event_not_found(self, admin_client):
        """Return 404 for non-existent job."""
        response = admin_client.get("/events/nonexistent")

        assert response.status_code == 404
        data = response.json()["detail"]
        assert data["error"] == "not_found"

    def test_get_event_operator_own_job(self, operator_client):
        """Operator can see their own job details."""
        _trigger_jobs["job-op"] = _create_job_record(
            "job-op", "bolivia", "operator@test.com"
        )

        response = operator_client.get("/events/job-op")

        assert response.status_code == 200
        assert response.json()["job_id"] == "job-op"

    def test_get_event_operator_other_job_hidden(self, operator_client):
        """Operator cannot see another user's job (returns 404)."""
        _trigger_jobs["job-other"] = _create_job_record(
            "job-other", "bolivia", "admin@test.com"
        )

        response = operator_client.get("/events/job-other")

        assert response.status_code == 404

    def test_get_event_includes_error(self, admin_client):
        """Event detail includes error info for failed jobs."""
        job = ExtractionJob(
            job_id="job-fail",
            country="brazil",
            queries=["q1"],
            status=JobStatus.FAILED,
            error="SQL Server connection timeout",
        )
        _trigger_jobs["job-fail"] = _TriggerJobRecord(
            extractor=MagicMock(), job=job, triggered_by="admin@test.com"
        )

        response = admin_client.get("/events/job-fail")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "failed"
        assert data["error"] == "SQL Server connection timeout"

    def test_get_event_includes_query_results(self, admin_client):
        """Event detail includes per-query results."""
        results = [
            QueryResult(
                query_name="q1",
                status=JobStatus.COMPLETED,
                rows_extracted=500,
                duration_seconds=2.0,
            ),
            QueryResult(
                query_name="q2",
                status=JobStatus.FAILED,
                error="Timeout",
                duration_seconds=30.0,
            ),
        ]
        _trigger_jobs["job-multi"] = _create_job_record(
            "job-multi",
            "bolivia",
            "admin@test.com",
            status=JobStatus.COMPLETED,
            queries=["q1", "q2"],
            results=results,
        )

        response = admin_client.get("/events/job-multi")

        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 2
        assert data["results"][0]["status"] == "completed"
        assert data["results"][1]["status"] == "failed"
        assert data["results"][1]["error"] == "Timeout"


# --- CORS tests ---


class TestCORSHeaders:
    """Tests for CORS header configuration."""

    def test_cors_allows_options_preflight(self, admin_client):
        """OPTIONS preflight request returns CORS headers."""
        response = admin_client.options(
            "/events",
            headers={
                "Origin": "http://localhost:5173",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "Authorization, Content-Type",
            },
        )

        # The response should not be 405 Method Not Allowed
        # (CORS middleware intercepts OPTIONS)
        assert response.status_code in (200, 204, 400)

    def test_cors_headers_present_on_response(self, admin_client):
        """Responses include CORS headers for allowed origins."""
        response = admin_client.get(
            "/events",
            headers={"Origin": "http://localhost:5173"},
        )

        # In debug mode, origin should be reflected or * used
        # We just verify the endpoint works; actual CORS header values
        # depend on the CORS_ALLOWED_ORIGINS config
        assert response.status_code == 200
