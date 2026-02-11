"""FastAPI application entry point."""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from sql_databricks_bridge import __version__
from sql_databricks_bridge.api.routes import auth, extract, health, jobs, metadata, sync, trigger
from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.jobs_table import ensure_jobs_table
from sql_databricks_bridge.db.sql_server import SQLServerClient
from sql_databricks_bridge.sync.poller import EventPoller

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global poller instance
_event_poller: EventPoller | None = None
_poller_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan handler."""
    global _event_poller, _poller_task

    settings = get_settings()
    logger.info(f"Starting {settings.service_name} v{__version__}")
    logger.info(f"Environment: {settings.environment}")

    # Start event poller if configured
    if settings.databricks.warehouse_id:
        try:
            databricks_client = DatabricksClient()
            sql_client = SQLServerClient()

            # Ensure the jobs Delta table exists
            ensure_jobs_table(databricks_client, settings.jobs_table)

            # Store client on app.state for route access
            app.state.databricks_client = databricks_client

            _event_poller = EventPoller(
                databricks_client=databricks_client,
                sql_client=sql_client,
                poll_interval=settings.polling_interval_seconds,
                max_events_per_poll=settings.max_events_per_poll,
            )

            # Start poller in background task
            _poller_task = asyncio.create_task(_event_poller.start())
            logger.info("Event poller started")

        except Exception as e:
            logger.warning(f"Failed to start event poller: {e}")
            _event_poller = None
    else:
        logger.info("Event poller disabled (no warehouse_id configured)")

    # Startup complete
    yield

    # Shutdown tasks
    logger.info("Shutting down...")

    if _event_poller:
        _event_poller.stop()

    if _poller_task:
        _poller_task.cancel()
        try:
            await _poller_task
        except asyncio.CancelledError:
            pass

    logger.info("Shutdown complete")


def create_app() -> FastAPI:
    """Create and configure FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title="SQL-Databricks Bridge",
        description="Bidirectional SQL Server ↔ Databricks data sync service",
        version=__version__,
        debug=settings.debug,
        lifespan=lifespan,
    )

    # CORS middleware
    if settings.cors_allowed_origins:
        origins = [o.strip() for o in settings.cors_allowed_origins.split(",")]
    elif settings.debug:
        origins = ["*"]
    else:
        origins = ["https://kantar-org.github.io"]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type"],
    )

    # Include routers under /api/v1 prefix
    api_v1 = APIRouter(prefix="/api/v1")
    api_v1.include_router(health.router)
    api_v1.include_router(auth.router)
    api_v1.include_router(metadata.router)
    api_v1.include_router(extract.router)
    api_v1.include_router(jobs.router)
    api_v1.include_router(sync.router)
    api_v1.include_router(trigger.router)
    app.include_router(api_v1)

    # Root endpoint
    @app.get("/", tags=["Root"])
    async def root() -> dict[str, str]:
        """Root endpoint with service info."""
        return {
            "service": settings.service_name,
            "version": __version__,
            "environment": settings.environment,
            "docs": "/docs",
        }

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "sql_databricks_bridge.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
    )
