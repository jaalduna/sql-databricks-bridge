"""Tests for /eligibility API endpoints."""

import os
import tempfile
import time
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from sql_databricks_bridge.auth.authorized_users import AuthorizedUser


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
        countries=["CO", "CL"],
    )


def _make_client(user: AuthorizedUser, tmp_dir: str):
    """Create a TestClient with auth and sqlite path overridden."""
    from sql_databricks_bridge.api.dependencies import get_current_azure_ad_user

    with patch("sql_databricks_bridge.main._event_poller", None):
        from sql_databricks_bridge.main import app

        app.state.sqlite_db_path = os.path.join(tmp_dir, "jobs.db")
        app.dependency_overrides[get_current_azure_ad_user] = lambda: user

        client = TestClient(app)
        yield client

        app.dependency_overrides.pop(get_current_azure_ad_user, None)


@pytest.fixture
def admin_client(admin_user, tmp_path):
    yield from _make_client(admin_user, str(tmp_path))


@pytest.fixture
def operator_client(operator_user, tmp_path):
    yield from _make_client(operator_user, str(tmp_path))


# --- POST /eligibility/runs ---


class TestCreateRun:
    def test_create_success(self, admin_client):
        response = admin_client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO", "period": 202401},
        )
        assert response.status_code == 201
        data = response.json()
        assert data["country"] == "CO"
        assert data["period"] == 202401
        assert data["status"] == "pending"
        assert data["run_id"]
        assert data["created_by"] == "admin@test.com"
        assert data["created_at"] is not None

    def test_create_with_parameters(self, admin_client):
        params = {"threshold": 0.8, "min_periods": 3}
        response = admin_client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CL", "period": 202402, "parameters": params},
        )
        assert response.status_code == 201
        data = response.json()
        assert data["parameters_json"] == params

    def test_create_missing_country(self, admin_client):
        response = admin_client.post(
            "/api/v1/eligibility/runs",
            json={"period": 202401},
        )
        assert response.status_code == 422

    def test_create_missing_period(self, admin_client):
        response = admin_client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO"},
        )
        assert response.status_code == 422

    def test_create_sets_created_by(self, operator_client):
        response = operator_client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO", "period": 202401},
        )
        assert response.status_code == 201
        assert response.json()["created_by"] == "operator@test.com"


# --- GET /eligibility/runs ---


class TestListRuns:
    def test_list_empty(self, admin_client):
        response = admin_client.get("/api/v1/eligibility/runs")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    def test_list_after_create(self, admin_client):
        admin_client.post("/api/v1/eligibility/runs", json={"country": "CO", "period": 202401})
        admin_client.post("/api/v1/eligibility/runs", json={"country": "CL", "period": 202401})

        response = admin_client.get("/api/v1/eligibility/runs")
        data = response.json()
        assert data["total"] == 2
        assert len(data["items"]) == 2

    def test_filter_by_country(self, admin_client):
        admin_client.post("/api/v1/eligibility/runs", json={"country": "CO", "period": 202401})
        admin_client.post("/api/v1/eligibility/runs", json={"country": "CL", "period": 202401})

        response = admin_client.get("/api/v1/eligibility/runs", params={"country": "CO"})
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["country"] == "CO"

    def test_filter_by_period(self, admin_client):
        admin_client.post("/api/v1/eligibility/runs", json={"country": "CO", "period": 202401})
        admin_client.post("/api/v1/eligibility/runs", json={"country": "CO", "period": 202402})

        response = admin_client.get("/api/v1/eligibility/runs", params={"period": 202401})
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["period"] == 202401

    def test_filter_by_status(self, admin_client):
        resp = admin_client.post(
            "/api/v1/eligibility/runs", json={"country": "CO", "period": 202401}
        )
        run_id = resp.json()["run_id"]
        admin_client.patch(
            f"/api/v1/eligibility/runs/{run_id}", json={"status": "running"}
        )
        admin_client.post("/api/v1/eligibility/runs", json={"country": "CO", "period": 202402})

        response = admin_client.get("/api/v1/eligibility/runs", params={"status": "running"})
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["status"] == "running"

    def test_pagination(self, admin_client):
        for i in range(5):
            admin_client.post(
                "/api/v1/eligibility/runs", json={"country": "CO", "period": 202400 + i}
            )

        response = admin_client.get(
            "/api/v1/eligibility/runs", params={"limit": 2, "offset": 0}
        )
        data = response.json()
        assert data["total"] == 5
        assert len(data["items"]) == 2

    def test_pagination_offset(self, admin_client):
        for i in range(5):
            admin_client.post(
                "/api/v1/eligibility/runs", json={"country": "CO", "period": 202400 + i}
            )

        response = admin_client.get(
            "/api/v1/eligibility/runs", params={"limit": 2, "offset": 4}
        )
        data = response.json()
        assert data["total"] == 5
        assert len(data["items"]) == 1


# --- GET /eligibility/runs/{run_id} ---


class TestGetRun:
    def test_get_success(self, admin_client):
        create_resp = admin_client.post(
            "/api/v1/eligibility/runs", json={"country": "CO", "period": 202401}
        )
        run_id = create_resp.json()["run_id"]

        response = admin_client.get(f"/api/v1/eligibility/runs/{run_id}")
        assert response.status_code == 200
        assert response.json()["run_id"] == run_id

    def test_get_not_found(self, admin_client):
        response = admin_client.get("/api/v1/eligibility/runs/nonexistent-id")
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "not_found"


# --- PATCH /eligibility/runs/{run_id} ---


class TestUpdateRun:
    def test_update_status(self, admin_client):
        create_resp = admin_client.post(
            "/api/v1/eligibility/runs", json={"country": "CO", "period": 202401}
        )
        run_id = create_resp.json()["run_id"]

        response = admin_client.patch(
            f"/api/v1/eligibility/runs/{run_id}",
            json={"status": "running"},
        )
        assert response.status_code == 200
        assert response.json()["status"] == "running"

    def test_update_to_approved(self, admin_client):
        create_resp = admin_client.post(
            "/api/v1/eligibility/runs", json={"country": "CO", "period": 202401}
        )
        run_id = create_resp.json()["run_id"]

        response = admin_client.patch(
            f"/api/v1/eligibility/runs/{run_id}",
            json={
                "status": "approved",
                "approved_by": "admin@test.com",
                "approval_comment": "Looks good",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "approved"
        assert data["approved_by"] == "admin@test.com"
        assert data["approval_comment"] == "Looks good"
        assert data["approved_at"] is not None

    def test_update_with_error(self, admin_client):
        create_resp = admin_client.post(
            "/api/v1/eligibility/runs", json={"country": "CO", "period": 202401}
        )
        run_id = create_resp.json()["run_id"]

        response = admin_client.patch(
            f"/api/v1/eligibility/runs/{run_id}",
            json={"status": "failed", "error_message": "Databricks job failed"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "failed"
        assert data["error_message"] == "Databricks job failed"

    def test_update_phase1_metrics(self, admin_client):
        create_resp = admin_client.post(
            "/api/v1/eligibility/runs", json={"country": "CO", "period": 202401}
        )
        run_id = create_resp.json()["run_id"]

        metrics = {"eligible_count": 1500, "total_count": 2000, "rate": 0.75}
        response = admin_client.patch(
            f"/api/v1/eligibility/runs/{run_id}",
            json={"status": "phase1_complete", "phase1_metrics_json": metrics},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "phase1_complete"
        assert data["phase1_metrics_json"] == metrics

    def test_update_not_found(self, admin_client):
        response = admin_client.patch(
            "/api/v1/eligibility/runs/nonexistent-id",
            json={"status": "running"},
        )
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "not_found"

    def test_update_updates_updated_at(self, admin_client):
        create_resp = admin_client.post(
            "/api/v1/eligibility/runs", json={"country": "CO", "period": 202401}
        )
        run_id = create_resp.json()["run_id"]
        original_updated_at = create_resp.json()["updated_at"]

        import time
        time.sleep(0.01)

        response = admin_client.patch(
            f"/api/v1/eligibility/runs/{run_id}",
            json={"status": "running"},
        )
        assert response.json()["updated_at"] >= original_updated_at


# --- DELETE /eligibility/runs/{run_id} ---


class TestDeleteRun:
    def test_delete_success(self, admin_client):
        create_resp = admin_client.post(
            "/api/v1/eligibility/runs", json={"country": "CO", "period": 202401}
        )
        run_id = create_resp.json()["run_id"]

        response = admin_client.delete(f"/api/v1/eligibility/runs/{run_id}")
        assert response.status_code == 204

        # Should be gone
        response = admin_client.get(f"/api/v1/eligibility/runs/{run_id}")
        assert response.status_code == 404

    def test_delete_not_found(self, admin_client):
        response = admin_client.delete("/api/v1/eligibility/runs/nonexistent-id")
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "not_found"

    def test_delete_removes_from_list(self, admin_client):
        r1 = admin_client.post(
            "/api/v1/eligibility/runs", json={"country": "CO", "period": 202401}
        )
        r2 = admin_client.post(
            "/api/v1/eligibility/runs", json={"country": "CL", "period": 202401}
        )
        run_id1 = r1.json()["run_id"]

        admin_client.delete(f"/api/v1/eligibility/runs/{run_id1}")

        response = admin_client.get("/api/v1/eligibility/runs")
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["country"] == "CL"


# --- Full lifecycle test ---


class TestEligibilityLifecycle:
    def test_full_lifecycle(self, admin_client):
        # 1. Create run
        resp = admin_client.post(
            "/api/v1/eligibility/runs",
            json={
                "country": "CO",
                "period": 202401,
                "parameters": {"min_periods": 6},
            },
        )
        assert resp.status_code == 201
        run_id = resp.json()["run_id"]
        assert resp.json()["status"] == "pending"

        # 2. Move to running
        resp = admin_client.patch(
            f"/api/v1/eligibility/runs/{run_id}", json={"status": "running"}
        )
        assert resp.json()["status"] == "running"

        # 3. Phase 1 complete with metrics
        metrics = {"eligible": 1200, "total": 2000}
        resp = admin_client.patch(
            f"/api/v1/eligibility/runs/{run_id}",
            json={"status": "phase1_complete", "phase1_metrics_json": metrics},
        )
        assert resp.json()["status"] == "phase1_complete"
        assert resp.json()["phase1_metrics_json"] == metrics

        # 4. Approve
        resp = admin_client.patch(
            f"/api/v1/eligibility/runs/{run_id}",
            json={
                "status": "approved",
                "approved_by": "admin@test.com",
                "approval_comment": "Approved for production",
            },
        )
        assert resp.json()["status"] == "approved"
        assert resp.json()["approved_at"] is not None

        # 5. Verify via GET
        resp = admin_client.get(f"/api/v1/eligibility/runs/{run_id}")
        data = resp.json()
        assert data["status"] == "approved"
        assert data["parameters_json"] == {"min_periods": 6}
        assert data["created_by"] == "admin@test.com"

        # 6. Delete
        resp = admin_client.delete(f"/api/v1/eligibility/runs/{run_id}")
        assert resp.status_code == 204


# --- New workflow tests ---


class TestCreateRunWithSteps:
    def test_create_initializes_steps(self, admin_client):
        resp = admin_client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO", "period": 202401},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert len(data["steps"]) == 8
        assert all(s["status"] == "pending" for s in data["steps"])
        step_names = [s["name"] for s in data["steps"]]
        assert step_names == [
            "run_pipeline", "download_results", "approve",
            "apply_sql", "download_mordom", "upload_mordom",
            "apply_mordom", "complete",
        ]
        assert data["current_step"] == "run_pipeline"

    def test_get_run_returns_steps_and_files(self, admin_client):
        resp = admin_client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO", "period": 202401},
        )
        run_id = resp.json()["run_id"]
        resp = admin_client.get(f"/api/v1/eligibility/runs/{run_id}")
        data = resp.json()
        assert "steps" in data
        assert "files" in data
        assert len(data["steps"]) == 8
        assert len(data["files"]) == 0


class TestExecutePipeline:
    def test_execute_returns_running(self, admin_client):
        resp = admin_client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO", "period": 202401},
        )
        run_id = resp.json()["run_id"]

        resp = admin_client.post(f"/api/v1/eligibility/runs/{run_id}/execute")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "running"
        assert data["started_at"] is not None

    def test_execute_completes_to_results_ready(self, admin_client):
        resp = admin_client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO", "period": 202401},
        )
        run_id = resp.json()["run_id"]

        admin_client.post(f"/api/v1/eligibility/runs/{run_id}/execute")

        for _ in range(10):
            time.sleep(0.5)
            resp = admin_client.get(f"/api/v1/eligibility/runs/{run_id}")
            if resp.json()["status"] == "results_ready":
                break

        data = resp.json()
        assert data["status"] == "results_ready"
        assert data["current_step"] == "approve"

    def test_execute_wrong_status(self, admin_client):
        resp = admin_client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO", "period": 202401},
        )
        run_id = resp.json()["run_id"]

        admin_client.patch(
            f"/api/v1/eligibility/runs/{run_id}",
            json={"status": "results_ready"},
        )

        resp = admin_client.post(f"/api/v1/eligibility/runs/{run_id}/execute")
        assert resp.status_code == 409
        assert resp.json()["detail"]["error"] == "invalid_status"


class TestListFiles:
    def test_files_empty_initially(self, admin_client):
        resp = admin_client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO", "period": 202401},
        )
        run_id = resp.json()["run_id"]

        resp = admin_client.get(f"/api/v1/eligibility/runs/{run_id}/files")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_files_after_execute(self, admin_client):
        resp = admin_client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO", "period": 202401},
        )
        run_id = resp.json()["run_id"]

        admin_client.post(f"/api/v1/eligibility/runs/{run_id}/execute")

        for _ in range(10):
            time.sleep(0.5)
            resp = admin_client.get(f"/api/v1/eligibility/runs/{run_id}")
            if resp.json()["status"] == "results_ready":
                break

        resp = admin_client.get(f"/api/v1/eligibility/runs/{run_id}/files")
        assert resp.status_code == 200
        files = resp.json()
        assert len(files) == 1
        assert files[0]["stage"] == 1
        assert files[0]["direction"] == "download"
        assert "resultados" in files[0]["filename"]


class TestDownloadFile:
    def test_download_file(self, admin_client):
        resp = admin_client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO", "period": 202401},
        )
        run_id = resp.json()["run_id"]

        admin_client.post(f"/api/v1/eligibility/runs/{run_id}/execute")

        for _ in range(10):
            time.sleep(0.5)
            resp = admin_client.get(f"/api/v1/eligibility/runs/{run_id}")
            if resp.json()["status"] == "results_ready":
                break

        resp = admin_client.get(f"/api/v1/eligibility/runs/{run_id}/files")
        file_id = resp.json()[0]["file_id"]

        resp = admin_client.get(
            f"/api/v1/eligibility/runs/{run_id}/files/{file_id}/download"
        )
        assert resp.status_code == 200
        assert "panelistID" in resp.text
        assert "text/csv" in resp.headers["content-type"]


class TestApprove:
    def _advance_to_results_ready(self, client):
        resp = client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO", "period": 202401},
        )
        run_id = resp.json()["run_id"]
        client.post(f"/api/v1/eligibility/runs/{run_id}/execute")
        for _ in range(10):
            time.sleep(0.5)
            resp = client.get(f"/api/v1/eligibility/runs/{run_id}")
            if resp.json()["status"] == "results_ready":
                break
        return run_id

    def test_approve_triggers_sql_apply(self, admin_client):
        run_id = self._advance_to_results_ready(admin_client)

        resp = admin_client.post(f"/api/v1/eligibility/runs/{run_id}/approve")
        assert resp.status_code == 200
        assert resp.json()["status"] == "applying_sql"
        assert resp.json()["approved_by"] == "admin@test.com"

    def test_approve_reaches_mordom_ready(self, admin_client):
        run_id = self._advance_to_results_ready(admin_client)

        admin_client.post(f"/api/v1/eligibility/runs/{run_id}/approve")

        for _ in range(20):
            time.sleep(0.5)
            resp = admin_client.get(f"/api/v1/eligibility/runs/{run_id}")
            if resp.json()["status"] == "mordom_ready":
                break

        assert resp.json()["status"] == "mordom_ready"
        # Mordom download file should exist
        files = admin_client.get(f"/api/v1/eligibility/runs/{run_id}/files").json()
        mordom_files = [f for f in files if f["stage"] == 2 and f["direction"] == "download"]
        assert len(mordom_files) == 1
        assert "mordom" in mordom_files[0]["filename"]

    def test_approve_wrong_status(self, admin_client):
        resp = admin_client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO", "period": 202401},
        )
        run_id = resp.json()["run_id"]

        resp = admin_client.post(f"/api/v1/eligibility/runs/{run_id}/approve")
        assert resp.status_code == 409


class TestMordomUpload:
    def _advance_to_mordom_ready(self, client):
        resp = client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO", "period": 202401},
        )
        run_id = resp.json()["run_id"]
        client.post(f"/api/v1/eligibility/runs/{run_id}/execute")
        for _ in range(10):
            time.sleep(0.5)
            resp = client.get(f"/api/v1/eligibility/runs/{run_id}")
            if resp.json()["status"] == "results_ready":
                break
        client.post(f"/api/v1/eligibility/runs/{run_id}/approve")
        for _ in range(20):
            time.sleep(0.5)
            resp = client.get(f"/api/v1/eligibility/runs/{run_id}")
            if resp.json()["status"] == "mordom_ready":
                break
        return run_id

    def test_upload_mordom_csv(self, admin_client):
        run_id = self._advance_to_mordom_ready(admin_client)

        csv_content = b"panelistID,action\nPAN001,keep\nPAN002,remove\n"
        resp = admin_client.post(
            f"/api/v1/eligibility/runs/{run_id}/upload",
            files={"file": ("mordom_corrected.csv", csv_content, "text/csv")},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "mordom_uploaded"
        assert resp.json()["current_step"] == "apply_mordom"

    def test_upload_wrong_status(self, admin_client):
        resp = admin_client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO", "period": 202401},
        )
        run_id = resp.json()["run_id"]

        csv_content = b"col1,col2\nval1,val2\n"
        resp = admin_client.post(
            f"/api/v1/eligibility/runs/{run_id}/upload",
            files={"file": ("test.csv", csv_content, "text/csv")},
        )
        assert resp.status_code == 409


class TestApplyMordom:
    def _advance_to_mordom_uploaded(self, client):
        resp = client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO", "period": 202401},
        )
        run_id = resp.json()["run_id"]
        client.post(f"/api/v1/eligibility/runs/{run_id}/execute")
        for _ in range(10):
            time.sleep(0.5)
            resp = client.get(f"/api/v1/eligibility/runs/{run_id}")
            if resp.json()["status"] == "results_ready":
                break
        client.post(f"/api/v1/eligibility/runs/{run_id}/approve")
        for _ in range(20):
            time.sleep(0.5)
            resp = client.get(f"/api/v1/eligibility/runs/{run_id}")
            if resp.json()["status"] == "mordom_ready":
                break
        csv_content = b"panelistID,action\nPAN001,keep\nPAN002,remove\n"
        client.post(
            f"/api/v1/eligibility/runs/{run_id}/upload",
            files={"file": ("mordom_corrected.csv", csv_content, "text/csv")},
        )
        return run_id

    def test_apply_mordom_marks_ready(self, admin_client):
        run_id = self._advance_to_mordom_uploaded(admin_client)

        resp = admin_client.post(f"/api/v1/eligibility/runs/{run_id}/apply-mordom")
        assert resp.status_code == 200
        assert resp.json()["status"] == "applying_mordom"

        for _ in range(10):
            time.sleep(0.5)
            resp = admin_client.get(f"/api/v1/eligibility/runs/{run_id}")
            if resp.json()["status"] == "ready":
                break

        data = resp.json()
        assert data["status"] == "ready"
        assert data["completed_at"] is not None
        assert data["current_step"] == "complete"

    def test_apply_mordom_wrong_status(self, admin_client):
        resp = admin_client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO", "period": 202401},
        )
        run_id = resp.json()["run_id"]

        resp = admin_client.post(f"/api/v1/eligibility/runs/{run_id}/apply-mordom")
        assert resp.status_code == 409


class TestCancel:
    def test_cancel_from_pending(self, admin_client):
        resp = admin_client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO", "period": 202401},
        )
        run_id = resp.json()["run_id"]

        resp = admin_client.post(f"/api/v1/eligibility/runs/{run_id}/cancel")
        assert resp.status_code == 200
        assert resp.json()["status"] == "cancelled"
        assert resp.json()["completed_at"] is not None

    def test_cancel_from_ready_fails(self, admin_client):
        resp = admin_client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CO", "period": 202401},
        )
        run_id = resp.json()["run_id"]

        admin_client.patch(
            f"/api/v1/eligibility/runs/{run_id}",
            json={"status": "ready"},
        )

        resp = admin_client.post(f"/api/v1/eligibility/runs/{run_id}/cancel")
        assert resp.status_code == 409


class TestFullLifecycle:
    def test_full_workflow(self, admin_client):
        """Full lifecycle: create → execute → approve → upload mordom → apply → ready."""
        # 1. Create
        resp = admin_client.post(
            "/api/v1/eligibility/runs",
            json={"country": "CL", "period": 202406, "parameters": {"min_periods": 6}},
        )
        assert resp.status_code == 201
        run_id = resp.json()["run_id"]
        assert resp.json()["status"] == "pending"
        assert len(resp.json()["steps"]) == 8

        # 2. Execute pipeline
        resp = admin_client.post(f"/api/v1/eligibility/runs/{run_id}/execute")
        assert resp.json()["status"] == "running"

        # 3. Wait for results_ready
        for _ in range(10):
            time.sleep(0.5)
            resp = admin_client.get(f"/api/v1/eligibility/runs/{run_id}")
            if resp.json()["status"] == "results_ready":
                break
        assert resp.json()["status"] == "results_ready"

        # 4. Download results file
        resp = admin_client.get(f"/api/v1/eligibility/runs/{run_id}/files")
        assert len(resp.json()) == 1
        file_id = resp.json()[0]["file_id"]
        resp = admin_client.get(f"/api/v1/eligibility/runs/{run_id}/files/{file_id}/download")
        assert resp.status_code == 200
        assert "panelistID" in resp.text

        # 5. Approve → triggers SQL apply + MorDom download
        resp = admin_client.post(f"/api/v1/eligibility/runs/{run_id}/approve")
        assert resp.json()["status"] == "applying_sql"

        # 6. Wait for mordom_ready
        for _ in range(20):
            time.sleep(0.5)
            resp = admin_client.get(f"/api/v1/eligibility/runs/{run_id}")
            if resp.json()["status"] == "mordom_ready":
                break
        assert resp.json()["status"] == "mordom_ready"

        # 7. Download MorDom file
        resp = admin_client.get(f"/api/v1/eligibility/runs/{run_id}/files")
        files = resp.json()
        mordom_files = [f for f in files if f["stage"] == 2 and f["direction"] == "download"]
        assert len(mordom_files) == 1

        # 8. Upload corrected MorDom
        csv_data = b"panelistID,action\nPAN001,keep\nPAN002,remove\n"
        resp = admin_client.post(
            f"/api/v1/eligibility/runs/{run_id}/upload",
            files={"file": ("mordom_corrected.csv", csv_data, "text/csv")},
        )
        assert resp.json()["status"] == "mordom_uploaded"

        # 9. Apply MorDom corrections
        resp = admin_client.post(f"/api/v1/eligibility/runs/{run_id}/apply-mordom")
        assert resp.json()["status"] == "applying_mordom"

        # 10. Wait for ready
        for _ in range(10):
            time.sleep(0.5)
            resp = admin_client.get(f"/api/v1/eligibility/runs/{run_id}")
            if resp.json()["status"] == "ready":
                break

        data = resp.json()
        assert data["status"] == "ready"
        assert data["completed_at"] is not None
        assert data["current_step"] == "complete"
        assert len(data["steps"]) == 8
        assert len(data["files"]) == 3  # 1 results + 1 mordom download + 1 mordom upload
        assert data["parameters_json"] == {"min_periods": 6}
