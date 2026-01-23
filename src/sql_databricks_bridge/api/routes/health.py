"""Health check endpoints."""

from enum import Enum
from typing import Any

from fastapi import APIRouter, status
from pydantic import BaseModel

from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.sql_server import SQLServerClient

router = APIRouter(prefix="/health", tags=["Health"])


class HealthStatus(str, Enum):
    """Health check status values."""

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"


class ComponentHealth(BaseModel):
    """Health status for a single component."""

    status: HealthStatus
    message: str | None = None
    details: dict[str, Any] | None = None


class HealthResponse(BaseModel):
    """Overall health response."""

    status: HealthStatus
    version: str
    environment: str
    components: dict[str, ComponentHealth]


@router.get(
    "/live",
    status_code=status.HTTP_200_OK,
    summary="Liveness probe",
    description="Simple check that the service is running.",
)
async def liveness() -> dict[str, str]:
    """Kubernetes liveness probe - just confirms service is running."""
    return {"status": "ok"}


@router.get(
    "/ready",
    response_model=HealthResponse,
    summary="Readiness probe",
    description="Check that all dependencies are available.",
)
async def readiness() -> HealthResponse:
    """Kubernetes readiness probe - checks all dependencies."""
    settings = get_settings()
    components: dict[str, ComponentHealth] = {}

    # Check SQL Server
    sql_client = SQLServerClient()
    try:
        sql_healthy = sql_client.test_connection()
        components["sql_server"] = ComponentHealth(
            status=HealthStatus.HEALTHY if sql_healthy else HealthStatus.UNHEALTHY,
            message="Connected" if sql_healthy else "Connection failed",
            details={"host": settings.sql_server.host, "database": settings.sql_server.database},
        )
    except Exception as e:
        components["sql_server"] = ComponentHealth(
            status=HealthStatus.UNHEALTHY,
            message=str(e),
        )
    finally:
        sql_client.close()

    # Check Databricks
    databricks_client = DatabricksClient()
    try:
        db_healthy = databricks_client.test_connection()
        components["databricks"] = ComponentHealth(
            status=HealthStatus.HEALTHY if db_healthy else HealthStatus.UNHEALTHY,
            message="Connected" if db_healthy else "Connection failed",
            details={"host": settings.databricks.host, "catalog": settings.databricks.catalog},
        )
    except Exception as e:
        components["databricks"] = ComponentHealth(
            status=HealthStatus.UNHEALTHY,
            message=str(e),
        )

    # Determine overall status
    statuses = [c.status for c in components.values()]
    if all(s == HealthStatus.HEALTHY for s in statuses):
        overall_status = HealthStatus.HEALTHY
    elif any(s == HealthStatus.UNHEALTHY for s in statuses):
        overall_status = HealthStatus.UNHEALTHY
    else:
        overall_status = HealthStatus.DEGRADED

    return HealthResponse(
        status=overall_status,
        version="0.1.0",
        environment=settings.environment,
        components=components,
    )


@router.get(
    "/startup",
    status_code=status.HTTP_200_OK,
    summary="Startup probe",
    description="Check that the service has started successfully.",
)
async def startup() -> dict[str, str]:
    """Kubernetes startup probe."""
    return {"status": "started"}
