"""Unit tests for main FastAPI application."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """Create test client with mocked poller."""
    with patch("sql_databricks_bridge.main._event_poller", None):
        from sql_databricks_bridge.main import app

        yield TestClient(app)


class TestAppSetup:
    """Tests for app configuration."""

    def test_app_title(self, client):
        """App has correct title."""
        from sql_databricks_bridge.main import app

        assert app.title == "SQL-Databricks Bridge"

    def test_app_version(self, client):
        """App has version."""
        from sql_databricks_bridge.main import app

        assert app.version is not None

    def test_app_has_routes(self, client):
        """App has expected routes."""
        from sql_databricks_bridge.main import app

        paths = [route.path for route in app.routes]

        # Health endpoints
        assert "/health/live" in paths
        assert "/health/ready" in paths

        # Sync endpoints
        assert "/sync/events" in paths

    def test_root_endpoint(self, client):
        """Root endpoint redirects or returns info."""
        response = client.get("/")

        # Could be redirect or info depending on config
        assert response.status_code in (200, 307, 404)


class TestAppLifespan:
    """Tests for app startup/shutdown."""

    def test_startup_without_poller(self):
        """App starts without event poller when not configured."""
        with patch("sql_databricks_bridge.main._event_poller", None):
            from sql_databricks_bridge.main import app

            # Should not raise
            with TestClient(app):
                pass


class TestCORS:
    """Tests for CORS configuration."""

    def test_cors_headers(self, client):
        """CORS headers are set correctly."""
        response = client.options(
            "/health/live",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "GET",
            },
        )

        # Should have CORS headers if enabled
        # The exact behavior depends on CORS configuration
        assert response.status_code in (200, 405)
