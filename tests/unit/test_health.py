"""Unit tests for health check endpoints."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from sql_databricks_bridge.api.routes.health import (
    ComponentHealth,
    HealthResponse,
    HealthStatus,
)


@pytest.fixture
def client():
    """Create test client."""
    with patch("sql_databricks_bridge.main._event_poller", None):
        from sql_databricks_bridge.main import app

        yield TestClient(app)


class TestHealthModels:
    """Tests for health check models."""

    def test_health_status_values(self):
        """Health status has expected values."""
        assert HealthStatus.HEALTHY == "healthy"
        assert HealthStatus.UNHEALTHY == "unhealthy"
        assert HealthStatus.DEGRADED == "degraded"

    def test_component_health_minimal(self):
        """Create component health with minimal fields."""
        health = ComponentHealth(status=HealthStatus.HEALTHY)
        assert health.status == HealthStatus.HEALTHY
        assert health.message is None
        assert health.details is None

    def test_component_health_full(self):
        """Create component health with all fields."""
        health = ComponentHealth(
            status=HealthStatus.HEALTHY,
            message="Connected",
            details={"host": "localhost"},
        )
        assert health.message == "Connected"
        assert health.details["host"] == "localhost"

    def test_health_response_model(self):
        """Create health response."""
        response = HealthResponse(
            status=HealthStatus.HEALTHY,
            version="1.0.0",
            environment="test",
            components={
                "sql_server": ComponentHealth(status=HealthStatus.HEALTHY),
            },
        )
        assert response.version == "1.0.0"
        assert len(response.components) == 1


class TestLivenessEndpoint:
    """Tests for /health/live endpoint."""

    def test_liveness_returns_ok(self, client):
        """Liveness probe returns ok."""
        response = client.get("/health/live")

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


class TestStartupEndpoint:
    """Tests for /health/startup endpoint."""

    def test_startup_returns_started(self, client):
        """Startup probe returns started."""
        response = client.get("/health/startup")

        assert response.status_code == 200
        assert response.json() == {"status": "started"}


class TestReadinessEndpoint:
    """Tests for /health/ready endpoint."""

    def test_readiness_healthy(self):
        """Readiness returns healthy when all components are up."""
        with patch("sql_databricks_bridge.main._event_poller", None):
            with patch(
                "sql_databricks_bridge.api.routes.health.SQLServerClient"
            ) as mock_sql:
                with patch(
                    "sql_databricks_bridge.api.routes.health.DatabricksClient"
                ) as mock_db:
                    # Both clients healthy
                    mock_sql_instance = MagicMock()
                    mock_sql_instance.test_connection.return_value = True
                    mock_sql.return_value = mock_sql_instance

                    mock_db_instance = MagicMock()
                    mock_db_instance.test_connection.return_value = True
                    mock_db.return_value = mock_db_instance

                    from sql_databricks_bridge.main import app

                    client = TestClient(app)
                    response = client.get("/health/ready")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_readiness_unhealthy_sql(self):
        """Readiness returns unhealthy when SQL Server is down."""
        with patch("sql_databricks_bridge.main._event_poller", None):
            with patch(
                "sql_databricks_bridge.api.routes.health.SQLServerClient"
            ) as mock_sql:
                with patch(
                    "sql_databricks_bridge.api.routes.health.DatabricksClient"
                ) as mock_db:
                    # SQL unhealthy
                    mock_sql_instance = MagicMock()
                    mock_sql_instance.test_connection.return_value = False
                    mock_sql.return_value = mock_sql_instance

                    mock_db_instance = MagicMock()
                    mock_db_instance.test_connection.return_value = True
                    mock_db.return_value = mock_db_instance

                    from sql_databricks_bridge.main import app

                    client = TestClient(app)
                    response = client.get("/health/ready")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "unhealthy"
        assert data["components"]["sql_server"]["status"] == "unhealthy"

    def test_readiness_exception_handling(self):
        """Readiness handles exceptions gracefully."""
        with patch("sql_databricks_bridge.main._event_poller", None):
            with patch(
                "sql_databricks_bridge.api.routes.health.SQLServerClient"
            ) as mock_sql:
                with patch(
                    "sql_databricks_bridge.api.routes.health.DatabricksClient"
                ) as mock_db:
                    # SQL raises exception
                    mock_sql_instance = MagicMock()
                    mock_sql_instance.test_connection.side_effect = Exception(
                        "Connection error"
                    )
                    mock_sql.return_value = mock_sql_instance

                    mock_db_instance = MagicMock()
                    mock_db_instance.test_connection.return_value = True
                    mock_db.return_value = mock_db_instance

                    from sql_databricks_bridge.main import app

                    client = TestClient(app)
                    response = client.get("/health/ready")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "unhealthy"
        assert "Connection error" in data["components"]["sql_server"]["message"]
