"""FastAPI application entry point."""

import asyncio
import logging
import os
import uuid as _uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncIterator

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from sql_databricks_bridge import __version__
from sql_databricks_bridge.api.routes import auth, databricks_jobs, extract, health, jobs, metadata, pipeline, sync, tags, trigger
from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.core.stages import build_tag
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.jobs_table import ensure_jobs_table, insert_job, update_job_status
from sql_databricks_bridge.db.version_tags_table import ensure_version_tags_table
from sql_databricks_bridge.db.sql_server import SQLServerClient
from sql_databricks_bridge.core.calibration_launcher import CalibrationJobLauncher
from sql_databricks_bridge.core.databricks_monitor import DatabricksJobMonitor
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
_job_monitor: DatabricksJobMonitor | None = None
_monitor_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan handler."""
    global _event_poller, _poller_task, _job_monitor, _monitor_task

    settings = get_settings()
    logger.info(f"Starting {settings.service_name} v{__version__}")
    logger.info(f"Environment: {settings.environment}")

    # Start event poller if configured
    if settings.databricks.warehouse_id:
        try:
            databricks_client = DatabricksClient()
            sql_client = SQLServerClient()

            # Ensure the jobs and version_tags Delta tables exist
            ensure_jobs_table(databricks_client, settings.jobs_table)
            ensure_version_tags_table(databricks_client, settings.version_tags_table)

            # Seed fake availability data for e2e testing
            seed_availability = os.environ.get("SEED_AVAILABILITY", "").lower() in ("1", "true", "yes")
            if seed_availability:
                seed_country = os.environ.get("SEED_COUNTRY", "bolivia")
                now = datetime.utcnow()
                # Calculate current period as YYYYMM
                seed_period = os.environ.get("SEED_PERIOD", now.strftime("%Y%m"))

                for seed_stage in ("elegibilidad", "pesaje"):
                    seed_job_id = str(_uuid.uuid4())
                    seed_tag = build_tag(seed_country, seed_stage)
                    created = datetime(now.year, now.month, 1)  # first of month
                    try:
                        insert_job(
                            databricks_client,
                            settings.jobs_table,
                            job_id=seed_job_id,
                            country=seed_country,
                            stage=seed_stage,
                            tag=seed_tag,
                            queries=["seed_placeholder"],
                            triggered_by="system-seed",
                            created_at=created,
                            period=seed_period,
                        )
                        update_job_status(
                            databricks_client,
                            settings.jobs_table,
                            seed_job_id,
                            "completed",
                            started_at=created,
                            completed_at=created,
                        )
                        logger.info(f"SEED_AVAILABILITY: Seeded {seed_stage} job for {seed_country} period {seed_period}")
                    except Exception as e:
                        logger.warning(f"SEED_AVAILABILITY: Failed to seed {seed_stage}: {e}")

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

            # Create calibration job launcher and expose to trigger module
            launcher = CalibrationJobLauncher(databricks_client)
            from sql_databricks_bridge.api.routes import trigger as trigger_module
            trigger_module._calibration_launcher = launcher

            # Start calibration job monitor
            _job_monitor = DatabricksJobMonitor(
                databricks_client=databricks_client,
                poll_interval=settings.polling_interval_seconds,
                launcher=launcher,
            )
            _monitor_task = asyncio.create_task(_job_monitor.start())
            logger.info("Calibration job monitor started")

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

    if _job_monitor:
        _job_monitor.stop()

    if _monitor_task:
        _monitor_task.cancel()
        try:
            await _monitor_task
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
    api_v1.include_router(tags.router)
    api_v1.include_router(trigger.router)
    api_v1.include_router(pipeline.router)
    api_v1.include_router(databricks_jobs.router)
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
