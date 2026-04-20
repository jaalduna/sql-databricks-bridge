"""FastAPI application entry point."""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from sql_databricks_bridge import __version__
from sql_databricks_bridge.api.routes import auth, databricks_jobs, diff_sync_schedule, eligibility, extract, health, jobs, metadata, phase2, pipeline, sync, tags, traceability, trigger
from sql_databricks_bridge.api.routes.auth import github_router
from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.jobs_table import ensure_jobs_table
from sql_databricks_bridge.db.local_store import init_db, mark_orphaned_jobs
from sql_databricks_bridge.db.version_tags_table import ensure_version_tags_table
from sql_databricks_bridge.db.traceability_table import ensure_traceability_tables
from sql_databricks_bridge.core.calibration_launcher import CalibrationJobLauncher
from sql_databricks_bridge.core.databricks_monitor import DatabricksJobMonitor
from sql_databricks_bridge.db.sql_server import SQLServerClient
from sql_databricks_bridge.sync.poller import EventPoller

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def _validate_poller_warehouse(
    client: DatabricksClient, warehouse_id: str
) -> bool:
    """Verify the poller warehouse exists and is reachable.

    Called at startup when a dedicated poller warehouse is configured.
    Uses ``warehouses.get`` which does NOT start the warehouse (cheap,
    no cold-start cost), only checks credentials + id validity.

    Returns True on success, False on any failure (logged as warning so
    startup does not crash — the poller will still try to run; the fail
    shows up early rather than on first poll_cycle).
    """
    try:
        await asyncio.to_thread(client.client.warehouses.get, warehouse_id)
        return True
    except Exception as e:
        logger.warning(
            "Poller warehouse %s validation failed: %s", warehouse_id, e
        )
        return False

_event_poller: EventPoller | None = None
_job_monitor: DatabricksJobMonitor | None = None
_monitor_task: asyncio.Task | None = None
_scheduler_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan handler."""
    global _event_poller, _job_monitor, _monitor_task, _scheduler_task

    settings = get_settings()
    logger.info(f"Starting {settings.service_name} v{__version__}")
    logger.info(f"Environment: {settings.environment}")

    # Start event poller if configured
    if settings.databricks.warehouse_id:
        try:
            databricks_client = DatabricksClient()

            # Ensure the jobs, version_tags and traceability Delta tables exist.
            # Skip on startup to avoid waking the SQL Warehouse — tables already
            # exist in production.  Set ENSURE_TABLES_ON_STARTUP=true to re-enable.
            if settings.ensure_tables_on_startup:
                ensure_jobs_table(databricks_client, settings.jobs_table)
                ensure_version_tags_table(databricks_client, settings.version_tags_table)
                ensure_traceability_tables(
                    databricks_client,
                    settings.traceability_tags_table,
                    settings.traceability_tag_tables_table,
                )
            else:
                logger.info("Skipping ensure_*_table DDL (ENSURE_TABLES_ON_STARTUP=false)")

            # Initialize local SQLite store and recover orphaned jobs
            db_path = init_db(settings.sqlite_db_path)
            mark_orphaned_jobs(db_path)
            app.state.sqlite_db_path = db_path

            # Store client on app.state for route access
            app.state.databricks_client = databricks_client

            # EventPoller: always create instance (on-demand endpoint always works).
            sql_client = SQLServerClient()
            # If a dedicated poller warehouse is configured, build a
            # second DatabricksClient pointing at it so EventPoller runs
            # in isolation from diff-sync/trigger traffic — the poller's
            # public API stays unchanged.
            poller_db_client = databricks_client
            poller_warehouse_id = settings.databricks.poller_warehouse_id
            if poller_warehouse_id and poller_warehouse_id != settings.databricks.warehouse_id:
                poller_settings = settings.databricks.model_copy(
                    update={"warehouse_id": poller_warehouse_id}
                )
                poller_db_client = DatabricksClient(settings=poller_settings)
                logger.info(
                    "Event poller will use dedicated warehouse: %s",
                    poller_warehouse_id,
                )
                # S3: fail-fast on misconfiguration instead of waiting for
                # the first poll_cycle to blow up.
                await _validate_poller_warehouse(poller_db_client, poller_warehouse_id)

            _event_poller = EventPoller(
                databricks_client=poller_db_client,
                sql_client=sql_client,
                events_table=settings.events_table,
                poll_interval=settings.polling_interval_seconds,
                max_events_per_poll=settings.max_events_per_poll,
            )
            logger.info("Event poller ready (on-demand: POST /api/v1/sync/poller/trigger)")

            # Start unified scheduler from schedules.yaml
            from sql_databricks_bridge.core.scheduler import TaskScheduler

            scheduler = TaskScheduler(settings.schedules_file)

            # Register event_poller handler
            async def _handle_event_poller(entry) -> None:
                n = await _event_poller.poll_cycle()
                logger.info("Event poller: %d events processed", n)

            scheduler.register("event_poller", _handle_event_poller)

            # Register diff_sync handler
            async def _handle_diff_sync(entry) -> None:
                from sql_databricks_bridge.core.diff_sync_runner import (
                    discover_countries, run_diff_sync_round,
                )
                api_base = f"http://127.0.0.1:{settings.api_port}/api/v1"
                countries = entry.extra.get("countries") or discover_countries(api_base)
                stage = entry.extra.get("stage", "sincronizacion")
                lookback = entry.extra.get("lookback_months", 24)
                deferred = entry.extra.get("deferred_phase2", False)
                all_tables = entry.extra.get("all_tables", False)
                table_suffix = entry.extra.get("table_suffix") or None
                await asyncio.to_thread(
                    run_diff_sync_round, api_base, countries, stage, lookback,
                    10, None, table_suffix, all_tables, None, deferred,
                )

            scheduler.register("diff_sync", _handle_diff_sync)

            _scheduler_task = asyncio.create_task(scheduler.start())
            app.state.scheduler = scheduler

            # Create calibration job launcher and expose to trigger module
            launcher = CalibrationJobLauncher(
                databricks_client,
                job_name_prefix=settings.calibration_job_prefix,
            )
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
            logger.warning(f"Failed to initialize Databricks services: {e}")
    else:
        logger.info("Databricks services disabled (no warehouse_id configured)")

    # Startup complete
    yield

    # Shutdown tasks
    logger.info("Shutting down...")

    if hasattr(app.state, "scheduler"):
        app.state.scheduler.stop()

    if _scheduler_task:
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except asyncio.CancelledError:
            pass

    if _event_poller:
        _event_poller.stop()

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
        expose_headers=["Content-Disposition"],
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
    api_v1.include_router(eligibility.router)
    api_v1.include_router(diff_sync_schedule.router)
    api_v1.include_router(phase2.router)
    api_v1.include_router(traceability.router)
    app.include_router(api_v1)

    # GitHub OAuth routes are mounted at root level (not under /api/v1) so the
    # frontend can redirect to {serverRoot}/auth/github after stripping /api/v1.
    app.include_router(github_router)

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
