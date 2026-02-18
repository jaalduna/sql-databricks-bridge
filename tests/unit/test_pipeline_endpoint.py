"""Unit tests for /pipeline API endpoints."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from sql_databricks_bridge.auth.authorized_users import AuthorizedUser
from sql_databricks_bridge.core.pipeline_tracker import pipeline_tracker


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


@pytest.fixture
def viewer_user():
    return AuthorizedUser(
        email="viewer@test.com",
        name="Viewer",
        roles=["viewer"],
        countries=["bolivia"],
    )


def _make_client(user: AuthorizedUser | None = None):
    """Create a TestClient with the Azure AD dependency overridden."""
    from sql_databricks_bridge.api.dependencies import get_current_azure_ad_user

    # Reset pipeline tracker state between tests
    pipeline_tracker._pipelines.clear()

    with patch("sql_databricks_bridge.main._event_poller", None):
        from sql_databricks_bridge.main import app

        mock_db_client = MagicMock()
        mock_db_client.execute_sql.return_value = []
        app.state.databricks_client = mock_db_client

        if user is not None:
            app.dependency_overrides[get_current_azure_ad_user] = lambda: user
        else:
            app.dependency_overrides.pop(get_current_azure_ad_user, None)

        client = TestClient(app)
        yield client

        app.dependency_overrides.pop(get_current_azure_ad_user, None)
        pipeline_tracker._pipelines.clear()


@pytest.fixture
def admin_client(admin_user):
    yield from _make_client(admin_user)


@pytest.fixture
def operator_client(operator_user):
    yield from _make_client(operator_user)


@pytest.fixture
def viewer_client(viewer_user):
    yield from _make_client(viewer_user)


# --- POST /pipeline ---


class TestCreatePipeline:
    """Tests for POST /pipeline."""

    def test_create_success(self, admin_client):
        response = admin_client.post(
            "/api/v1/pipeline",
            json={"country": "bolivia"},
        )
        assert response.status_code == 201
        data = response.json()
        assert data["country"] == "bolivia"
        assert data["status"] == "pending"
        assert len(data["steps"]) == 6
        assert data["pipeline_id"]

    def test_create_with_period(self, admin_client):
        response = admin_client.post(
            "/api/v1/pipeline",
            json={"country": "bolivia", "period": "202602"},
        )
        assert response.status_code == 201
        assert response.json()["period"] == "202602"

    def test_create_with_sync_queries(self, admin_client):
        response = admin_client.post(
            "/api/v1/pipeline",
            json={
                "country": "bolivia",
                "sync_queries": ["j_atoscompra_new", "hato_cabecalho"],
            },
        )
        assert response.status_code == 201
        data = response.json()
        sync_step = data["steps"][0]
        assert sync_step["step_id"] == "sync"
        assert sync_step["substeps_total"] == 2
        assert sync_step["substeps"][0]["name"] == "j_atoscompra_new"

    def test_create_steps_have_correct_structure(self, admin_client):
        response = admin_client.post(
            "/api/v1/pipeline",
            json={"country": "bolivia"},
        )
        data = response.json()
        step_ids = [s["step_id"] for s in data["steps"]]
        assert step_ids == [
            "sync",
            "copy_bronze",
            "merge",
            "simulate_weights",
            "simulate_kpis",
            "calculate_targets",
        ]
        # KPI substeps
        kpi_step = data["steps"][4]
        assert kpi_step["substeps_total"] == 3
        kpi_names = [s["name"] for s in kpi_step["substeps"]]
        assert kpi_names == ["kpi_all", "kpi_bc", "kpi_original"]

    def test_create_triggered_by_set(self, admin_client):
        response = admin_client.post(
            "/api/v1/pipeline",
            json={"country": "bolivia"},
        )
        assert response.json()["triggered_by"] == "admin@test.com"

    def test_create_forbidden_viewer(self, viewer_client):
        response = viewer_client.post(
            "/api/v1/pipeline",
            json={"country": "bolivia"},
        )
        assert response.status_code == 403

    def test_create_forbidden_country(self, operator_client):
        response = operator_client.post(
            "/api/v1/pipeline",
            json={"country": "brazil"},
        )
        assert response.status_code == 403

    def test_operator_allowed_country(self, operator_client):
        response = operator_client.post(
            "/api/v1/pipeline",
            json={"country": "bolivia"},
        )
        assert response.status_code == 201


# --- GET /pipeline ---


class TestListPipelines:
    """Tests for GET /pipeline."""

    def test_list_empty(self, admin_client):
        response = admin_client.get("/api/v1/pipeline")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    def test_list_after_create(self, admin_client):
        admin_client.post("/api/v1/pipeline", json={"country": "bolivia"})
        admin_client.post("/api/v1/pipeline", json={"country": "chile"})

        response = admin_client.get("/api/v1/pipeline")
        data = response.json()
        assert data["total"] == 2
        assert len(data["items"]) == 2

    def test_list_filter_by_country(self, admin_client):
        admin_client.post("/api/v1/pipeline", json={"country": "bolivia"})
        admin_client.post("/api/v1/pipeline", json={"country": "chile"})

        response = admin_client.get("/api/v1/pipeline", params={"country": "bolivia"})
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["country"] == "bolivia"

    def test_list_pagination(self, admin_client):
        for _ in range(5):
            admin_client.post("/api/v1/pipeline", json={"country": "bolivia"})

        response = admin_client.get("/api/v1/pipeline", params={"limit": 2, "offset": 0})
        data = response.json()
        assert data["total"] == 5
        assert len(data["items"]) == 2


# --- GET /pipeline/{pipeline_id} ---


class TestGetPipeline:
    """Tests for GET /pipeline/{pipeline_id}."""

    def test_get_success(self, admin_client):
        create_resp = admin_client.post("/api/v1/pipeline", json={"country": "bolivia"})
        pid = create_resp.json()["pipeline_id"]

        response = admin_client.get(f"/api/v1/pipeline/{pid}")
        assert response.status_code == 200
        assert response.json()["pipeline_id"] == pid

    def test_get_not_found(self, admin_client):
        response = admin_client.get("/pipeline/nonexistent-id")
        assert response.status_code == 404


# --- PATCH /pipeline/{id}/steps/{step_id} ---


class TestUpdateStep:
    """Tests for PATCH /pipeline/{id}/steps/{step_id}."""

    def test_start_step(self, admin_client):
        create_resp = admin_client.post("/api/v1/pipeline", json={"country": "bolivia"})
        pid = create_resp.json()["pipeline_id"]

        response = admin_client.patch(
            f"/api/v1/pipeline/{pid}/steps/sync",
            json={"status": "running"},
        )
        assert response.status_code == 200
        data = response.json()
        sync_step = data["steps"][0]
        assert sync_step["status"] == "running"
        assert sync_step["started_at"] is not None
        assert data["status"] == "running"

    def test_complete_step(self, admin_client):
        create_resp = admin_client.post("/api/v1/pipeline", json={"country": "bolivia"})
        pid = create_resp.json()["pipeline_id"]

        admin_client.patch(f"/api/v1/pipeline/{pid}/steps/sync", json={"status": "running"})
        response = admin_client.patch(
            f"/api/v1/pipeline/{pid}/steps/sync",
            json={"status": "completed"},
        )
        assert response.status_code == 200
        sync_step = response.json()["steps"][0]
        assert sync_step["status"] == "completed"
        assert sync_step["completed_at"] is not None

    def test_fail_step(self, admin_client):
        create_resp = admin_client.post("/api/v1/pipeline", json={"country": "bolivia"})
        pid = create_resp.json()["pipeline_id"]

        admin_client.patch(f"/api/v1/pipeline/{pid}/steps/sync", json={"status": "running"})
        response = admin_client.patch(
            f"/api/v1/pipeline/{pid}/steps/sync",
            json={"status": "failed", "error": "Connection refused"},
        )
        assert response.status_code == 200
        sync_step = response.json()["steps"][0]
        assert sync_step["status"] == "failed"
        assert sync_step["error"] == "Connection refused"

    def test_skip_step(self, admin_client):
        create_resp = admin_client.post("/api/v1/pipeline", json={"country": "bolivia"})
        pid = create_resp.json()["pipeline_id"]

        response = admin_client.patch(
            f"/api/v1/pipeline/{pid}/steps/simulate_weights",
            json={"status": "skipped"},
        )
        assert response.status_code == 200
        weights_step = response.json()["steps"][3]
        assert weights_step["status"] == "skipped"

    def test_attach_databricks_run_id(self, admin_client):
        create_resp = admin_client.post("/api/v1/pipeline", json={"country": "bolivia"})
        pid = create_resp.json()["pipeline_id"]

        response = admin_client.patch(
            f"/api/v1/pipeline/{pid}/steps/merge",
            json={"status": "running", "databricks_run_id": 12345},
        )
        assert response.status_code == 200
        merge_step = response.json()["steps"][2]
        assert merge_step["databricks_run_id"] == 12345
        assert merge_step["status"] == "running"

    def test_update_step_not_found(self, admin_client):
        create_resp = admin_client.post("/api/v1/pipeline", json={"country": "bolivia"})
        pid = create_resp.json()["pipeline_id"]

        response = admin_client.patch(
            f"/api/v1/pipeline/{pid}/steps/nonexistent",
            json={"status": "running"},
        )
        assert response.status_code == 404

    def test_update_step_pipeline_not_found(self, admin_client):
        response = admin_client.patch(
            "/pipeline/nonexistent/steps/sync",
            json={"status": "running"},
        )
        assert response.status_code == 404


# --- PATCH /pipeline/{id}/steps/{step_id}/substeps/{name} ---


class TestUpdateSubStep:
    """Tests for PATCH /pipeline/{id}/steps/{step_id}/substeps/{name}."""

    def test_update_substep_running(self, admin_client):
        create_resp = admin_client.post(
            "/api/v1/pipeline",
            json={"country": "bolivia", "sync_queries": ["q1", "q2"]},
        )
        pid = create_resp.json()["pipeline_id"]

        response = admin_client.patch(
            f"/api/v1/pipeline/{pid}/steps/sync/substeps/q1",
            json={"status": "running"},
        )
        assert response.status_code == 200
        sync_step = response.json()["steps"][0]
        q1 = sync_step["substeps"][0]
        assert q1["status"] == "running"
        assert q1["started_at"] is not None

    def test_update_substep_completed_with_rows(self, admin_client):
        create_resp = admin_client.post(
            "/api/v1/pipeline",
            json={"country": "bolivia", "sync_queries": ["q1"]},
        )
        pid = create_resp.json()["pipeline_id"]

        admin_client.patch(
            f"/api/v1/pipeline/{pid}/steps/sync/substeps/q1",
            json={"status": "running"},
        )
        response = admin_client.patch(
            f"/api/v1/pipeline/{pid}/steps/sync/substeps/q1",
            json={"status": "completed", "rows_affected": 5000},
        )
        assert response.status_code == 200
        q1 = response.json()["steps"][0]["substeps"][0]
        assert q1["status"] == "completed"
        assert q1["rows_affected"] == 5000

    def test_update_substep_not_found(self, admin_client):
        create_resp = admin_client.post(
            "/api/v1/pipeline",
            json={"country": "bolivia", "sync_queries": ["q1"]},
        )
        pid = create_resp.json()["pipeline_id"]

        response = admin_client.patch(
            f"/api/v1/pipeline/{pid}/steps/sync/substeps/nonexistent",
            json={"status": "running"},
        )
        assert response.status_code == 404


# --- POST /pipeline/{id}/poll ---


class TestPollPipeline:
    """Tests for POST /pipeline/{id}/poll."""

    def test_poll_returns_pipeline(self, admin_client):
        create_resp = admin_client.post("/api/v1/pipeline", json={"country": "bolivia"})
        pid = create_resp.json()["pipeline_id"]

        response = admin_client.post(f"/api/v1/pipeline/{pid}/poll")
        assert response.status_code == 200
        assert response.json()["pipeline_id"] == pid

    def test_poll_not_found(self, admin_client):
        response = admin_client.post("/pipeline/nonexistent/poll")
        assert response.status_code == 404


# --- DELETE /pipeline/{id} ---


class TestDeletePipeline:
    """Tests for DELETE /pipeline/{id}."""

    def test_delete_admin(self, admin_client):
        create_resp = admin_client.post("/api/v1/pipeline", json={"country": "bolivia"})
        pid = create_resp.json()["pipeline_id"]

        response = admin_client.delete(f"/api/v1/pipeline/{pid}")
        assert response.status_code == 204

        # Should be gone
        response = admin_client.get(f"/api/v1/pipeline/{pid}")
        assert response.status_code == 404

    def test_delete_forbidden_non_admin(self, operator_client):
        # Create as operator
        create_resp = operator_client.post("/api/v1/pipeline", json={"country": "bolivia"})
        pid = create_resp.json()["pipeline_id"]

        response = operator_client.delete(f"/api/v1/pipeline/{pid}")
        assert response.status_code == 403

    def test_delete_not_found(self, admin_client):
        response = admin_client.delete("/pipeline/nonexistent")
        assert response.status_code == 404


# --- Full pipeline lifecycle test ---


class TestPipelineLifecycle:
    """End-to-end test of a full pipeline lifecycle through the API."""

    def test_full_lifecycle(self, admin_client):
        # 1. Create pipeline
        resp = admin_client.post(
            "/api/v1/pipeline",
            json={
                "country": "bolivia",
                "period": "202602",
                "sync_queries": ["j_atoscompra_new", "hato_cabecalho"],
            },
        )
        assert resp.status_code == 201
        pid = resp.json()["pipeline_id"]

        # 2. Start sync step
        resp = admin_client.patch(
            f"/api/v1/pipeline/{pid}/steps/sync",
            json={"status": "running"},
        )
        assert resp.json()["current_step"] == "sync"

        # 3. Update sync substeps
        admin_client.patch(
            f"/api/v1/pipeline/{pid}/steps/sync/substeps/j_atoscompra_new",
            json={"status": "running"},
        )
        resp = admin_client.patch(
            f"/api/v1/pipeline/{pid}/steps/sync/substeps/j_atoscompra_new",
            json={"status": "completed", "rows_affected": 1500},
        )
        sync_step = resp.json()["steps"][0]
        assert sync_step["substeps_completed"] == 1
        assert sync_step["progress_pct"] == 50.0  # 1/2 substeps

        admin_client.patch(
            f"/api/v1/pipeline/{pid}/steps/sync/substeps/hato_cabecalho",
            json={"status": "completed", "rows_affected": 800},
        )

        # 4. Complete sync step
        resp = admin_client.patch(
            f"/api/v1/pipeline/{pid}/steps/sync",
            json={"status": "completed"},
        )
        assert resp.json()["steps"][0]["status"] == "completed"
        assert resp.json()["steps_completed"] == 1

        # 5. Run through remaining steps
        for step_id in ["copy_bronze", "merge", "simulate_weights", "simulate_kpis", "calculate_targets"]:
            admin_client.patch(f"/api/v1/pipeline/{pid}/steps/{step_id}", json={"status": "running"})
            admin_client.patch(f"/api/v1/pipeline/{pid}/steps/{step_id}", json={"status": "completed"})

        # 6. Verify pipeline completed
        resp = admin_client.get(f"/api/v1/pipeline/{pid}")
        data = resp.json()
        assert data["status"] == "completed"
        assert data["steps_completed"] == 6
        assert data["progress_pct"] == 100.0
        assert data["completed_at"] is not None
