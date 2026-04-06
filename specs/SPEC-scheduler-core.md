# SPEC: Scheduler Core

**Slug**: `scheduler-core`
**Status**: implementing
**Created**: 2026-03-16
**Author**: Joaquin Aldunate
**PRD**: `PRD-sync-scheduler` (F-01)

---

## Problem

Data engineers must manually trigger SQL Server → Databricks sync jobs for 8+ countries. There is no automated scheduling mechanism in the bridge service. The `EventPoller` handles event-driven Databricks → SQL Server sync, but nothing periodically *initiates* extractions.

## Scope

### In Scope
- APScheduler 3.x `AsyncIOScheduler` integration with FastAPI lifespan
- `schedules.yaml` config file: cron expression + timezone per country
- Scheduler invokes the existing trigger extraction pipeline programmatically (no HTTP overhead)
- Retry with exponential backoff on sync failure (configurable max attempts)
- SQLite persistence for schedule state (enabled/paused, last run, last status)
- New `BridgeSettings` fields for scheduler configuration
- `max_instances=1` per country job (skip if previous run still active)
- Scheduler disabled when `skip_sync_data=True`
- Execution history purge on startup (90-day retention)

### Out of Scope
- REST API for schedule management (see `SPEC-schedule-api`)
- Webhook/notification on failure (see `SPEC-schedule-notifications`)
- Frontend UI
- Calibration pipeline scheduling
- Distributed/multi-instance coordination

## Requirements

> Each requirement is numbered, testable, and independent.

### Functional Requirements

1. **FR-01**: On startup, the scheduler reads `schedules.yaml` and registers one APScheduler `CronTrigger` job per country entry.
   - *Rationale*: Countries need independent schedules with timezone awareness.

2. **FR-02**: Each scheduled job invokes the existing extraction pipeline (`_run_trigger_extraction` logic from `trigger.py`) with the country's configured parameters.
   - *Rationale*: Reuse the proven extraction → delta write flow; no code duplication.

3. **FR-03**: If a scheduled sync fails, the scheduler retries with exponential backoff up to `max_retries` attempts (default: 3) with base delay (default: 60s).
   - *Rationale*: Transient SQL Server or network failures should self-heal.

4. **FR-04**: The scheduler persists each execution's result (success/failure, timestamp, error message) to a new `schedule_executions` SQLite table.
   - *Rationale*: Enables status monitoring and history without relying on in-memory state.

5. **FR-05**: The scheduler persists per-country schedule state (enabled/paused) to a `schedule_state` SQLite table. On startup, runtime state is merged with YAML defaults (runtime overrides take precedence).
   - *Rationale*: Supports pause/resume that survives service restarts.

6. **FR-06**: Each country job uses `max_instances=1` so that if a sync is still running when the next cron tick fires, the new execution is skipped (not queued).
   - *Rationale*: Prevents SQL Server connection pool exhaustion from overlapping syncs.

7. **FR-07**: The scheduler is disabled entirely when `skip_sync_data=True` or `scheduler_enabled=False`.
   - *Rationale*: Test environments without SQL Server access must not attempt scheduled syncs.

8. **FR-08**: On startup, execution records older than `scheduler_history_retention_days` (default: 90) are deleted from `schedule_executions`.
   - *Rationale*: Prevent unbounded SQLite growth.

### Non-Functional Requirements

1. **NFR-01**: The scheduler must not block the FastAPI event loop. All sync work runs as asyncio background tasks.
2. **NFR-02**: Scheduler startup/shutdown must integrate with the existing FastAPI `lifespan` context manager in `main.py`.
3. **NFR-03**: Default country schedules are staggered by 15 minutes to avoid concurrent SQL Server load.

## Data Flow

```
Service startup
    │
    ├── Read schedules.yaml → list of {country, cron, timezone, params}
    ├── Read schedule_state from SQLite → merge overrides (paused countries)
    ├── Purge old execution records (>90 days)
    └── Register APScheduler CronTrigger jobs
            │
            On cron tick (per country):
            │
            ▼
    run_scheduled_sync(country, params)
            │
            ├── Create ExtractionJob via Extractor
            ├── Execute queries (SQL Server → Polars DataFrames)
            ├── Write to Databricks Delta tables via DeltaTableWriter
            ├── Persist job to SQLite trigger_jobs table (triggered_by="scheduler")
            └── Record execution result to schedule_executions table
                    │
                    On failure: retry (exponential backoff, max N attempts)
                    On final failure: record error, continue to next scheduled tick
```

## Technical Design

### New Files

- `src/sql_databricks_bridge/core/scheduler.py` — `SyncScheduler` class wrapping `AsyncIOScheduler`
- `src/sql_databricks_bridge/data/schedules.yaml` — Default schedule config
- `src/sql_databricks_bridge/db/schedule_store.py` — SQLite tables for schedule state + execution history

### Affected Files

- `src/sql_databricks_bridge/core/config.py` — New `BridgeSettings` fields
- `src/sql_databricks_bridge/main.py` — Start/stop scheduler in `lifespan`
- `src/sql_databricks_bridge/api/routes/trigger.py` — Extract `_run_trigger_extraction` core logic into a reusable function (or import directly)

### `schedules.yaml` Format

```yaml
schedules:
  - country: bolivia
    cron: "0 2 * * *"        # Daily at 02:00
    timezone: America/La_Paz
    stage: inicio
    enabled: true

  - country: chile
    cron: "0 2 15 * * *"     # Daily at 02:15 (staggered)
    timezone: America/Santiago
    stage: inicio
    enabled: true
```

### `BridgeSettings` New Fields

```python
scheduler_enabled: bool = True
schedules_file: str = ""  # Override path (empty = bundled default)
scheduler_max_retries: int = 3
scheduler_retry_base_delay: int = 60  # seconds
scheduler_history_retention_days: int = 90
```

### SQLite New Tables

```sql
CREATE TABLE IF NOT EXISTS schedule_state (
    country TEXT PRIMARY KEY,
    enabled INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schedule_executions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country TEXT NOT NULL,
    trigger_job_id TEXT,          -- FK to trigger_jobs.job_id
    status TEXT NOT NULL,         -- 'success', 'failed', 'skipped'
    started_at TEXT NOT NULL,
    completed_at TEXT,
    attempt INTEGER NOT NULL DEFAULT 1,
    error TEXT,
    FOREIGN KEY (trigger_job_id) REFERENCES trigger_jobs(job_id)
);
CREATE INDEX idx_sched_exec_country ON schedule_executions(country);
CREATE INDEX idx_sched_exec_started ON schedule_executions(started_at DESC);
```

### `SyncScheduler` Class (Key Interface)

```python
class SyncScheduler:
    def __init__(self, settings: BridgeSettings, db_path: str): ...
    async def start(self) -> None: ...       # Load config, register jobs, start scheduler
    async def stop(self) -> None: ...        # Graceful shutdown
    def pause_country(self, country: str) -> None: ...
    def resume_country(self, country: str) -> None: ...
    def get_status(self) -> list[ScheduleStatus]: ...  # Next run, last result per country
```

## Edge Cases & Constraints

1. **Missing `schedules.yaml`**: If the file doesn't exist and no override path is set, the scheduler starts with zero jobs and logs a warning. No crash.
2. **Unknown country in YAML**: Country names must match those recognized by `CountryAwareQueryLoader`. Invalid entries are logged and skipped.
3. **Overlapping manual + scheduled sync**: Both can run concurrently for the same country. The existing `trigger_jobs` table tracks both (distinguishable by `triggered_by` field: "scheduler" vs user email).
4. **Service restart mid-sync**: Existing `mark_orphaned_jobs()` in `local_store.py` handles this — orphaned jobs are marked failed on startup.
5. **All retries exhausted**: Execution is recorded as failed. The scheduler continues to the next cron tick (does not disable the country).

## Assumptions

- APScheduler 3.x `AsyncIOScheduler` is thread-safe for the single-instance use case.
- The extraction pipeline function can be called directly as a Python function (no HTTP request needed).
- `triggered_by="scheduler"` is sufficient to distinguish automated from manual syncs in all downstream queries.

## Acceptance Criteria

> These are the "definition of done". Each is verifiable.

1. [ ] **AC-01**: A `schedules.yaml` file exists with at least one country entry, and the scheduler registers cron jobs on startup.
2. [ ] **AC-02**: When a cron tick fires, the scheduler extracts data from SQL Server and writes it to Databricks Delta tables — identical output to a manual trigger.
3. [ ] **AC-03**: If extraction fails, the scheduler retries up to `scheduler_max_retries` times with exponential backoff before recording failure.
4. [ ] **AC-04**: Each execution (success, failure, skip) is recorded in the `schedule_executions` SQLite table.
5. [ ] **AC-05**: Per-country pause/resume state is persisted in `schedule_state` and survives service restarts.
6. [ ] **AC-06**: Setting `skip_sync_data=True` or `scheduler_enabled=False` prevents the scheduler from starting.
7. [ ] **AC-07**: Concurrent scheduled syncs for different countries do not exceed `max_parallel_queries` per individual sync.
8. [ ] **AC-08**: A manual sync can run simultaneously with a scheduled sync for the same country without conflict.
9. [ ] **AC-09**: Execution records older than `scheduler_history_retention_days` are purged on startup.
10. [ ] **AC-10**: Scheduler starts and stops cleanly within the FastAPI lifespan (no orphaned tasks on shutdown).

## Dependencies

- `SPEC-schedule-api` depends on this spec (provides the engine that the API controls)
- `SPEC-schedule-notifications` depends on this spec (hooks into failure events)
- APScheduler 3.x (new dependency in `pyproject.toml`)

## Estimated Effort

- **T-shirt size**: M
- **Suggested breakdown**: 1 phase, ~3-4 focused tasks (scheduler class, YAML loader, SQLite store, lifespan integration)

## Linear Issues

| Issue | Title | Requirements | Status |
|-------|-------|-------------|--------|
| JAA-105 | Add APScheduler dependency and scheduler settings to BridgeSettings | FR-07, NFR-02 | Backlog |
| JAA-106 | Create SQLite schedule tables and store module | FR-04, FR-05, FR-08 | Backlog |
| JAA-112 | Implement SyncScheduler engine with YAML config loader | FR-01, FR-02, FR-06, NFR-01, NFR-03 | Backlog |
| JAA-113 | Add retry logic with exponential backoff and lifespan integration | FR-03, NFR-02 | Backlog |
