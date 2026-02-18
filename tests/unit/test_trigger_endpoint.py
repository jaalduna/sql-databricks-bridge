"""Unit tests for POST /trigger endpoint."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from sql_databricks_bridge.auth.authorized_users import AuthorizedUser


# --- Fixtures ---


@pytest.fixture
def admin_user():
    """Admin user authorized for all countries."""
    return AuthorizedUser(
        email="admin@test.com",
        name="Admin",
        roles=["admin"],
        countries=["*"],
    )


@pytest.fixture
def operator_user():
    """Operator authorized for bolivia and chile only."""
    return AuthorizedUser(
        email="operator@test.com",
        name="Operator",
        roles=["operator"],
        countries=["bolivia", "chile"],
    )


@pytest.fixture
def viewer_user():
    """Viewer -- cannot trigger syncs."""
    return AuthorizedUser(
        email="viewer@test.com",
        name="Viewer",
        roles=["viewer"],
        countries=["bolivia"],
    )


def _make_client(user: AuthorizedUser | None = None) -> TestClient:
    """Create a TestClient with the Azure AD dependency overridden."""
    from sql_databricks_bridge.api.dependencies import get_current_azure_ad_user
    from sql_databricks_bridge.api.routes.trigger import _trigger_jobs

    _trigger_jobs.clear()

    with patch("sql_databricks_bridge.main._event_poller", None):
        from sql_databricks_bridge.main import app

        # Provide a mock DatabricksClient on app.state so routes can use it
        mock_db_client = MagicMock()
        mock_db_client.execute_sql.return_value = []
        app.state.databricks_client = mock_db_client

        if user is not None:
            app.dependency_overrides[get_current_azure_ad_user] = lambda: user
        else:
            # Clear overrides to require real auth
            app.dependency_overrides.pop(get_current_azure_ad_user, None)

        client = TestClient(app)
        yield client

        app.dependency_overrides.pop(get_current_azure_ad_user, None)
        _trigger_jobs.clear()


@pytest.fixture
def admin_client(admin_user):
    """TestClient authenticated as admin."""
    yield from _make_client(admin_user)


@pytest.fixture
def operator_client(operator_user):
    """TestClient authenticated as operator."""
    yield from _make_client(operator_user)


@pytest.fixture
def viewer_client(viewer_user):
    """TestClient authenticated as viewer."""
    yield from _make_client(viewer_user)


# --- Trigger endpoint tests ---


class TestTriggerExtraction:
    """Tests for POST /trigger."""

    @patch("sql_databricks_bridge.api.routes.trigger.DatabricksClient")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_trigger_success_admin(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockDatabricksClient,
        admin_client,
    ):
        """Admin can trigger extraction for any country."""
        mock_extractor = MagicMock()
        mock_job = MagicMock()
        mock_job.job_id = "job-123"
        mock_job.country = "bolivia"
        mock_job.queries = ["j_atoscompra_new"]
        mock_job.created_at = datetime.utcnow()
        mock_extractor.create_job.return_value = mock_job
        MockExtractor.return_value = mock_extractor

        response = admin_client.post(
            "/api/v1/trigger",
            json={"country": "bolivia", "stage": "calibracion"},
        )

        assert response.status_code == 201
        data = response.json()
        assert data["job_id"] == "job-123"
        assert data["country"] == "bolivia"
        assert data["stage"] == "calibracion"
        assert "tag" in data
        assert data["triggered_by"] == "admin@test.com"

    @patch("sql_databricks_bridge.api.routes.trigger.DatabricksClient")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_trigger_with_specific_queries(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockDatabricksClient,
        admin_client,
    ):
        """Trigger with specific queries passes them through."""
        mock_extractor = MagicMock()
        mock_job = MagicMock()
        mock_job.job_id = "job-456"
        mock_job.country = "chile"
        mock_job.queries = ["query_a", "query_b"]
        mock_job.created_at = datetime.utcnow()
        mock_extractor.create_job.return_value = mock_job
        MockExtractor.return_value = mock_extractor

        response = admin_client.post(
            "/api/v1/trigger",
            json={"country": "chile", "stage": "precios", "queries": ["query_a", "query_b"]},
        )

        assert response.status_code == 201
        data = response.json()
        assert data["queries"] == ["query_a", "query_b"]
        assert data["queries_count"] == 2

    @patch("sql_databricks_bridge.api.routes.trigger.DatabricksClient")
    @patch("sql_databricks_bridge.api.routes.trigger.DeltaTableWriter")
    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_trigger_operator_allowed_country(
        self,
        MockExtractor,
        MockSQLClient,
        MockDeltaWriter,
        MockDatabricksClient,
        operator_client,
    ):
        """Operator can trigger for authorized countries."""
        mock_extractor = MagicMock()
        mock_job = MagicMock()
        mock_job.job_id = "job-789"
        mock_job.country = "bolivia"
        mock_job.queries = ["q1"]
        mock_job.created_at = datetime.utcnow()
        mock_extractor.create_job.return_value = mock_job
        MockExtractor.return_value = mock_extractor

        response = operator_client.post(
            "/api/v1/trigger",
            json={"country": "bolivia", "stage": "pesaje"},
        )

        assert response.status_code == 201

    def test_trigger_operator_forbidden_country(self, operator_client):
        """Operator cannot trigger for unauthorized country."""
        response = operator_client.post(
            "/api/v1/trigger",
            json={"country": "brazil", "stage": "calibracion"},
        )

        assert response.status_code == 403
        data = response.json()["detail"]
        assert data["error"] == "forbidden"
        assert "brazil" in data["message"]

    def test_trigger_viewer_forbidden(self, viewer_client):
        """Viewer cannot trigger syncs at all."""
        response = viewer_client.post(
            "/api/v1/trigger",
            json={"country": "bolivia", "stage": "calibracion"},
        )

        assert response.status_code == 403
        data = response.json()["detail"]
        assert data["error"] == "forbidden"
        assert "role" in data["message"].lower()

    def test_trigger_no_auth_returns_401(self):
        """Request without auth token returns 401."""
        with patch("sql_databricks_bridge.main._event_poller", None):
            with patch(
                "sql_databricks_bridge.api.dependencies.get_settings"
            ) as mock_settings:
                mock_settings.return_value.auth_enabled = True
                mock_settings.return_value.azure_ad_tenant_id = "t"
                mock_settings.return_value.azure_ad_client_id = "c"
                mock_settings.return_value.authorized_users_file = "/nonexistent"
                mock_settings.return_value.permissions_file = "/nonexistent"
                mock_settings.return_value.permissions_hot_reload = False

                from sql_databricks_bridge.main import app

                # Clear overrides to use real auth
                app.dependency_overrides.clear()

                client = TestClient(app)
                response = client.post(
                    "/api/v1/trigger",
                    json={"country": "bolivia", "stage": "calibracion"},
                )

                # Should be 401 or 403 without valid token
                assert response.status_code in (401, 403)

    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_trigger_country_not_found(
        self,
        MockExtractor,
        MockSQLClient,
        admin_client,
    ):
        """Trigger for non-existent country returns 404 or 400."""
        mock_extractor = MagicMock()
        mock_extractor.create_job.side_effect = FileNotFoundError(
            "No queries found for country: nonexistent"
        )
        MockExtractor.return_value = mock_extractor

        response = admin_client.post(
            "/api/v1/trigger",
            json={"country": "nonexistent", "stage": "calibracion"},
        )

        assert response.status_code == 404
        assert "not_found" in response.json()["detail"]["error"]

    @patch("sql_databricks_bridge.api.routes.trigger.SQLServerClient")
    @patch("sql_databricks_bridge.api.routes.trigger.Extractor")
    def test_trigger_invalid_queries(
        self,
        MockExtractor,
        MockSQLClient,
        admin_client,
    ):
        """Trigger with invalid query names returns 400."""
        mock_extractor = MagicMock()
        mock_extractor.create_job.side_effect = ValueError(
            "Queries not available: ['nonexistent_query']"
        )
        MockExtractor.return_value = mock_extractor

        response = admin_client.post(
            "/api/v1/trigger",
            json={"country": "bolivia", "stage": "calibracion", "queries": ["nonexistent_query"]},
        )

        assert response.status_code == 400
        assert "validation_error" in response.json()["detail"]["error"]

    def test_trigger_missing_country_field(self, admin_client):
        """Missing required country field returns 422."""
        response = admin_client.post(
            "/api/v1/trigger",
            json={},
        )

        assert response.status_code == 422
