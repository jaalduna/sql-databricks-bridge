# SPEC: Schedule Management API

**Slug**: `schedule-api`
**Status**: implementing
**Created**: 2026-03-16
**Author**: Joaquin Aldunate
**PRD**: `PRD-sync-scheduler` (F-02)

---

## Problem

Once the scheduler core is running (SPEC-scheduler-core), data engineers have no way to manage schedules at runtime without editing YAML files and restarting the service. They need REST endpoints to pause/resume countries, view schedule status, check next run times, and review execution history — all without service interruption.

## Scope

### In Scope
- REST API endpoints under `/api/v1/schedules` for schedule management
- List all schedules with effective config (YAML merged with runtime overrides)
- Pause/resume a country's schedule
- Update a country's cron expression and timezone at runtime
- View next scheduled run time per country
- View execution history (last N runs per country, with status and errors)
- Runtime overrides persisted in SQLite (survive restarts, override YAML defaults)

### Out of Scope
- Creating entirely new country schedules that don't exist in YAML (YAML defines the country roster)
- Frontend UI (API-first; UI comes later)
- Deleting a country from the schedule (disable via pause instead)
- Webhook/notification configuration (see `SPEC-schedule-notifications`)

## Requirements

### Functional Requirements

1. **FR-01**: `GET /api/v1/schedules` returns a list of all country schedules with: country, cron expression, timezone, enabled/paused, next run time, last execution status.
   - *Rationale*: Single endpoint for engineers to see the full picture.

2. **FR-02**: `POST /api/v1/schedules/{country}/pause` pauses a country's schedule. The APScheduler job is paused and state is persisted to SQLite.
   - *Rationale*: Maintenance windows require halting individual countries without restart.

3. **FR-03**: `POST /api/v1/schedules/{country}/resume` resumes a paused country's schedule.
   - *Rationale*: Complement to pause.

4. **FR-04**: `PATCH /api/v1/schedules/{country}` accepts a JSON body with optional `cron` and `timezone` fields. Updates the APScheduler job in-place and persists the override to SQLite.
   - *Rationale*: Runtime cron changes without restart or YAML edits.

5. **FR-05**: `GET /api/v1/schedules/{country}/history` returns the last N execution records (default 20) from `schedule_executions`, ordered by most recent.
   - *Rationale*: Engineers need to review recent sync outcomes per country.

6. **FR-06**: `POST /api/v1/schedules/{country}/trigger` manually triggers an immediate sync for a country via the scheduler (bypasses cron wait).
   - *Rationale*: Supports US-07 — ad-hoc syncs through the scheduler interface.

7. **FR-07**: All schedule endpoints require authentication via the existing Azure AD middleware.
   - *Rationale*: Consistent with all other bridge API endpoints.

### Non-Functional Requirements

1. **NFR-01**: API responses return within 200ms for list/status endpoints (no heavy computation).
2. **NFR-02**: Pause/resume/update operations take effect immediately (no delay until next cron tick).

## Data Flow

```
Engineer → GET /api/v1/schedules
                │
                ▼
    SyncScheduler.get_status()
    ├── APScheduler: next_run_time per job
    ├── SQLite schedule_state: enabled/paused
    └── SQLite schedule_executions: last result
                │
                ▼
    JSON response: [{country, cron, tz, enabled, next_run, last_status, last_run_at}]
```

## Technical Design

### New Files

- `src/sql_databricks_bridge/api/routes/schedules.py` — FastAPI router with schedule endpoints
- `src/sql_databricks_bridge/api/schemas_schedule.py` — Pydantic request/response models

### Affected Files

- `src/sql_databricks_bridge/main.py` — Include the new `schedules` router
- `src/sql_databricks_bridge/core/scheduler.py` — Expose methods needed by the API (already designed in SPEC-scheduler-core)

### API Models

```python
class ScheduleInfo(BaseModel):
    country: str
    cron: str
    timezone: str
    enabled: bool
    next_run_at: datetime | None
    last_status: str | None       # "success" | "failed" | "skipped" | None
    last_run_at: datetime | None
    is_running: bool

class ScheduleUpdateRequest(BaseModel):
    cron: str | None = None
    timezone: str | None = None

class ScheduleExecutionRecord(BaseModel):
    id: int
    country: str
    trigger_job_id: str | None
    status: str
    started_at: datetime
    completed_at: datetime | None
    attempt: int
    error: str | None
```

### Endpoints Summary

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/schedules` | List all schedules with status |
| POST | `/api/v1/schedules/{country}/pause` | Pause country schedule |
| POST | `/api/v1/schedules/{country}/resume` | Resume country schedule |
| PATCH | `/api/v1/schedules/{country}` | Update cron/timezone |
| GET | `/api/v1/schedules/{country}/history` | Execution history |
| POST | `/api/v1/schedules/{country}/trigger` | Manual immediate trigger |

## Edge Cases & Constraints

1. **Pause an already-paused country**: Returns 200 (idempotent), no state change.
2. **Resume an already-active country**: Returns 200 (idempotent).
3. **Update cron for a paused country**: Persists the new cron but does not resume. Country stays paused with the new cron ready for when it's resumed.
4. **Unknown country in path**: Returns 404 with clear message.
5. **Invalid cron expression**: Returns 422 with validation error before persisting.
6. **Trigger while already running**: Returns 409 Conflict ("Sync already in progress for {country}").

## Assumptions

- The `SyncScheduler` class from SPEC-scheduler-core exposes the methods needed (`get_status`, `pause_country`, `resume_country`, `update_schedule`, `trigger_now`).
- The scheduler instance is accessible from FastAPI routes via `app.state.scheduler` (same pattern as `app.state.databricks_client`).

## Acceptance Criteria

1. [ ] **AC-01**: `GET /api/v1/schedules` returns all configured countries with correct cron, timezone, enabled status, next run time, and last execution info.
2. [ ] **AC-02**: `POST /api/v1/schedules/{country}/pause` stops the cron job and persists the paused state. After service restart, the country remains paused.
3. [ ] **AC-03**: `POST /api/v1/schedules/{country}/resume` restarts the cron job. Next run time is populated.
4. [ ] **AC-04**: `PATCH /api/v1/schedules/{country}` with a new cron expression updates the schedule immediately. Next run time reflects the new cron.
5. [ ] **AC-05**: `GET /api/v1/schedules/{country}/history` returns execution records ordered by most recent, with pagination support (limit/offset query params).
6. [ ] **AC-06**: `POST /api/v1/schedules/{country}/trigger` starts a sync immediately and returns the job_id.
7. [ ] **AC-07**: All endpoints return 401 without valid Azure AD token.
8. [ ] **AC-08**: Invalid cron expressions are rejected with 422 before any state change.

## Dependencies

- **SPEC-scheduler-core**: Must be implemented first. Provides `SyncScheduler` class and SQLite tables.

## Estimated Effort

- **T-shirt size**: M
- **Suggested breakdown**: 2 tasks (API routes + Pydantic models, integration tests)

## Linear Issues

| Issue | Title | Requirements | Status |
|-------|-------|-------------|--------|
| JAA-114 | Implement schedule management REST API endpoints | FR-01 through FR-07, NFR-01, NFR-02 | Backlog |
| JAA-116 | Integration tests for schedule management API | All FR/NFR | Backlog |
