# SPEC: Failure Notifications

**Slug**: `schedule-notifications`
**Status**: implementing
**Created**: 2026-03-16
**Author**: Joaquin Aldunate
**PRD**: `PRD-sync-scheduler` (F-03)

---

## Problem

When a scheduled sync fails after all retries, the failure is only recorded in SQLite. No one is actively alerted. Data engineers discover stale data only when a downstream calibration pipeline produces unexpected results — by then, the data may be hours or days old.

## Scope

### In Scope
- Webhook notification on final sync failure (all retries exhausted)
- Configurable webhook URL via `BridgeSettings`
- JSON payload with failure details (country, error, timestamps, retry count)
- Optional success notification (configurable, off by default)
- Async HTTP POST via `httpx` (already a project dependency)

### Out of Scope
- Email delivery (consumers of the webhook can forward to email)
- Slack/Teams specific formatting (webhook payload is generic JSON; Slack can consume it via a wrapper)
- Notification for manual trigger failures (scheduler-only)
- Notification preferences per country (global webhook URL for MVP)
- Retry of the notification itself (fire-and-forget with error logging)

## Requirements

### Functional Requirements

1. **FR-01**: When a scheduled sync fails after all retries, the scheduler sends an HTTP POST to the configured `scheduler_webhook_url` with a JSON payload.
   - *Rationale*: Engineers need timely alerts to prevent stale data from affecting calibration.

2. **FR-02**: The webhook payload includes: `event` ("sync_failed"), `country`, `error`, `started_at`, `failed_at`, `attempts`, `job_id`, `next_scheduled_run`.
   - *Rationale*: Enough context to triage without opening the bridge UI.

3. **FR-03**: If `scheduler_webhook_url` is empty or not set, notifications are silently skipped (no error).
   - *Rationale*: Notifications are optional; the scheduler must work without them.

4. **FR-04**: An optional `scheduler_notify_on_success` setting (default: `False`) enables success notifications with payload: `event` ("sync_completed"), `country`, `rows_synced`, `duration_seconds`, `job_id`.
   - *Rationale*: Some teams want positive confirmation, but most don't need the noise.

5. **FR-05**: Notification delivery is fire-and-forget with a 10-second timeout. Failures are logged at WARNING level but do not affect the scheduler or sync pipeline.
   - *Rationale*: A broken webhook must not cascade into missed syncs.

### Non-Functional Requirements

1. **NFR-01**: Notification HTTP POST must not block the scheduler event loop (use `httpx.AsyncClient`).
2. **NFR-02**: Webhook timeout of 10 seconds. No retries on notification delivery failure.

## Data Flow

```
Scheduled sync fails after N retries
        │
        ▼
SyncScheduler._on_final_failure(country, error, execution)
        │
        ├── Record to schedule_executions (already done by core)
        └── If scheduler_webhook_url is set:
                │
                ▼
            httpx.AsyncClient.post(webhook_url, json=payload, timeout=10)
                │
                ├── Success: log at DEBUG level
                └── Failure: log at WARNING level, continue
```

## Technical Design

### New Files

- `src/sql_databricks_bridge/core/notifier.py` — `ScheduleNotifier` class

### Affected Files

- `src/sql_databricks_bridge/core/config.py` — New settings fields
- `src/sql_databricks_bridge/core/scheduler.py` — Call notifier on final failure (and optionally on success)

### `BridgeSettings` New Fields

```python
scheduler_webhook_url: str = ""          # Empty = notifications disabled
scheduler_notify_on_success: bool = False
```

### Webhook Payload (Failure)

```json
{
  "event": "sync_failed",
  "service": "sql-databricks-bridge",
  "country": "bolivia",
  "job_id": "abc-123",
  "error": "ODBC connection timeout after 30s",
  "attempts": 3,
  "started_at": "2026-03-16T02:00:00Z",
  "failed_at": "2026-03-16T02:15:32Z",
  "next_scheduled_run": "2026-03-17T02:00:00Z"
}
```

### Webhook Payload (Success)

```json
{
  "event": "sync_completed",
  "service": "sql-databricks-bridge",
  "country": "bolivia",
  "job_id": "abc-123",
  "rows_synced": 145320,
  "duration_seconds": 342,
  "completed_at": "2026-03-16T02:05:42Z"
}
```

### `ScheduleNotifier` Class

```python
class ScheduleNotifier:
    def __init__(self, webhook_url: str, notify_on_success: bool = False): ...
    async def notify_failure(self, country: str, job_id: str, error: str,
                             attempts: int, started_at: datetime,
                             failed_at: datetime, next_run: datetime | None) -> None: ...
    async def notify_success(self, country: str, job_id: str,
                             rows_synced: int, duration_seconds: float,
                             completed_at: datetime) -> None: ...
```

## Edge Cases & Constraints

1. **Webhook URL returns non-2xx**: Logged at WARNING, no retry, no impact on scheduler.
2. **Webhook URL is unreachable (DNS/network)**: `httpx` raises after 10s timeout, logged, no impact.
3. **Webhook URL not configured**: `notify_failure` / `notify_success` return immediately (no-op).
4. **Very large error message**: Truncated to 1000 characters in the payload to avoid oversized requests.

## Assumptions

- `httpx.AsyncClient` is already available (project dependency).
- A single global webhook URL is sufficient for MVP (no per-country routing).
- The webhook consumer handles deduplication if needed (the bridge sends exactly once per event).

## Acceptance Criteria

1. [ ] **AC-01**: When a scheduled sync fails after all retries and `scheduler_webhook_url` is set, an HTTP POST is sent with the failure payload.
2. [ ] **AC-02**: When `scheduler_webhook_url` is empty, no HTTP request is made and no error is logged.
3. [ ] **AC-03**: If the webhook endpoint is unreachable or returns an error, the failure is logged at WARNING and the scheduler continues normally.
4. [ ] **AC-04**: When `scheduler_notify_on_success=True` and a sync completes, a success payload is sent.
5. [ ] **AC-05**: Notification delivery does not add more than 100ms to the scheduler's post-sync processing (fire-and-forget pattern).

## Dependencies

- **SPEC-scheduler-core**: Must be implemented first. Provides the failure/success hooks that trigger notifications.

## Estimated Effort

- **T-shirt size**: S
- **Suggested breakdown**: 1 task (notifier class + integration into scheduler + tests)

## Linear Issues

| Issue | Title | Requirements | Status |
|-------|-------|-------------|--------|
| JAA-115 | Implement webhook notifier for scheduled sync failures | FR-01 through FR-05, NFR-01, NFR-02 | Backlog |
