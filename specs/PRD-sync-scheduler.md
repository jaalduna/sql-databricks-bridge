# PRD: Periodic Sync Scheduler

**Slug**: `sync-scheduler`
**Status**: implementing
**Created**: 2026-03-16
**Author**: Joaquin Aldunate

---

## Vision

The SQL-Databricks Bridge runs data syncs only when manually triggered by a user through the frontend or API. This means data freshness depends entirely on human memory and availability. The desired end state is a system where each country's SQL Server data is automatically extracted and uploaded to Databricks on a configurable schedule, with per-country timezone awareness, so that downstream calibration pipelines always operate on fresh data without manual intervention.

## Problem Statement

Today, data engineers must manually trigger sync jobs for each country through the bridge UI or API. With 8+ countries in production, this creates:

1. **Human bottleneck** — syncs are delayed or forgotten when engineers are busy or unavailable
2. **Inconsistent data freshness** — some countries get synced daily, others lag behind
3. **Timezone mismatch** — countries operate in different timezones; a sensible sync time for Chile is midnight for another region, but manual triggers don't account for this
4. **No retry on failure** — if a manual sync fails, the engineer must notice and re-trigger it

The `EventPoller` (sync/poller.py) handles event-driven sync from Databricks, but there is no mechanism to periodically *initiate* extractions from SQL Server on a schedule.

## Goals & Success Metrics

| Goal | Metric | Target |
|------|--------|--------|
| Eliminate manual sync triggers for routine operations | % of syncs triggered by scheduler vs manual | >90% within 2 weeks of deployment |
| Ensure consistent data freshness across countries | Max age of data per country at calibration time | <24 hours |
| Reduce engineer toil | Manual sync triggers per week | <5 (down from ~40) |
| Reliable execution with failure recovery | Scheduled jobs that complete without manual intervention | >95% |

## Target Users

| User Type | Description | Primary Need |
|-----------|-------------|-------------|
| Data Engineers | Internal team managing SQL-Databricks pipelines for 8+ countries | Set-and-forget schedules that keep data fresh without daily manual work |

## User Stories

> Format: As a [user type], I want [action] so that [benefit].

1. **US-01**: As a data engineer, I want to define a cron schedule per country so that each country's data syncs automatically at the right local time.
2. **US-02**: As a data engineer, I want to pause/resume a country's schedule via the API so that I can temporarily halt syncs during maintenance without editing config files.
3. **US-03**: As a data engineer, I want failed syncs to retry automatically so that transient SQL Server or network issues don't require manual re-triggers.
4. **US-04**: As a data engineer, I want to receive a notification when a scheduled sync fails after all retries so that I can investigate before it affects calibration.
5. **US-05**: As a data engineer, I want to see the next scheduled run time and last execution status for each country so that I can monitor the system at a glance.
6. **US-06**: As a data engineer, I want YAML-based default schedules that are version-controlled so that schedule configuration is auditable and reproducible across environments.
7. **US-07**: As a data engineer, I want to manually trigger a sync at any time even when a schedule is active so that I can force a refresh after hotfixes or data corrections without disabling the scheduler.

## Proposed Solution

Add a scheduler layer to the bridge service using APScheduler that:

1. Reads default schedules from a `schedules.yaml` config file (cron expressions + timezone per country)
2. Exposes REST API endpoints to list, create, update, pause/resume, and delete schedules at runtime
3. Persists runtime schedule overrides in the existing SQLite local store
4. Triggers the same extraction pipeline used by the manual `/trigger/sync` endpoint
5. Implements configurable retry with exponential backoff on failure
6. Sends notifications (webhook) on final failure after retries exhausted

### Key Capabilities

1. **CAP-01**: Cron-based scheduling with per-country timezone support
2. **CAP-02**: YAML default config + API runtime overrides (API takes precedence)
3. **CAP-03**: Automatic retry with exponential backoff (configurable max attempts)
4. **CAP-04**: Failure notification via configurable webhook URL
5. **CAP-05**: Schedule status dashboard data (next run, last result, enabled/paused)
6. **CAP-06**: Graceful startup/shutdown integrated with FastAPI lifespan

## Scope

### In Scope
- Periodic scheduling of SQL Server → Databricks data extraction (the existing sync/trigger pipeline). **Note**: "Sync" in this PRD refers exclusively to the SQL Server → Databricks extraction pipeline (trigger endpoint), not the event-driven Databricks → SQL Server poller (`EventPoller`).
- Per-country cron expressions with timezone awareness
- YAML config file for default schedules
- REST API for CRUD + pause/resume of schedules
- Retry logic with configurable attempts and backoff
- Webhook notification on final failure
- Schedule state persistence in local SQLite
- Schedule status API (next run, last run, last status)

### Out of Scope
- Scheduling calibration pipeline steps (future enhancement, separate from sync)
- Frontend UI for schedule management (API-first; UI can follow in a later spec)
- Distributed scheduler / multi-instance coordination (single-instance service for now)
- Email notification delivery (webhook only for MVP; email can wrap the webhook)

### Future Considerations
- Frontend schedule management UI with calendar view
- Calibration pipeline scheduling (chain: sync completes → trigger calibration)
- Multi-instance leader election for HA deployments
- Slack/Teams integration for notifications

## System Context

```
[schedules.yaml]  ──defaults──→  [Scheduler Engine (APScheduler)]
                                        │
[REST API]  ──runtime overrides──→      │
                                        │
                                   On cron tick:
                                        │
                                        ▼
                            [Existing Trigger Pipeline]
                            (Extractor → DeltaTableWriter)
                                        │
                                   On failure:
                                   retry (N times)
                                        │
                                   On final failure:
                                        ▼
                              [Webhook Notification]
```

## Risks & Mitigations

| Risk | Impact | Likelihood | Mitigation |
|------|--------|-----------|------------|
| APScheduler adds complexity to the FastAPI event loop | Med | Med | Use APScheduler's `AsyncIOScheduler` which integrates natively with asyncio |
| Overlapping syncs if previous run hasn't finished | High | Med | Use `max_instances=1` per job in APScheduler (skips if still running) |
| SQLite concurrent access from scheduler + API | Low | Low | Use WAL mode (already in use) + single-writer pattern |
| Schedule config drift between YAML and runtime overrides | Med | Med | API clearly documents precedence: runtime overrides > YAML defaults. Status endpoint shows effective config |
| Long-running syncs block the scheduler thread | High | Low | Syncs run as background tasks (existing pattern in trigger.py), scheduler only enqueues |
| SQL Server connection pool exhaustion from concurrent scheduled syncs | High | Med | Stagger country schedules by default (15-min intervals), respect `max_parallel_queries` setting, `max_instances=1` prevents overlap per country |

## Assumptions

- The bridge service runs as a single instance per environment (no distributed coordination needed)
- APScheduler 3.x (`AsyncIOScheduler`) is the chosen scheduler library — battle-tested, widely documented, native asyncio integration. 4.x was considered but has fewer production references and a different API surface.
- Existing trigger pipeline (`/trigger/sync/{country}`) can be invoked programmatically without HTTP overhead
- Webhook is sufficient for MVP notifications (no email/Slack requirement)
- Per-country schedules are independent (no cross-country ordering or dependencies)
- Manual triggers coexist with scheduled triggers — the scheduler does not lock or block ad-hoc syncs
- Scheduler is disabled when `skip_sync_data=True` (test environments without SQL Server access)
- Execution history retained for 90 days by default (configurable via `BridgeSettings`). Older records are purged on scheduler startup.

## Dependencies

- **APScheduler 3.x**: Scheduler library (new dependency, `AsyncIOScheduler`)
- **Existing trigger pipeline**: `trigger.py` extraction logic (reuse, no modification expected)
- **Local SQLite store**: `local_store.py` for persisting schedule overrides and execution history
- **BridgeSettings**: New config fields for scheduler enable/disable and notification webhook URL

## Feature Breakdown

> These are candidate features that will each become a separate spec (via `/spec`).

| # | Feature | Priority | Complexity | Spec |
|---|---------|----------|-----------|------|
| F-01 | Scheduler Core (APScheduler engine + YAML config + retry + SQLite persistence + FastAPI lifespan) | Must-have | M | `SPEC-scheduler-core` |
| F-02 | Schedule Management API (CRUD + pause/resume + status + next run / last run history) | Must-have | M | `SPEC-schedule-api` |
| F-03 | Failure Notifications (webhook on final failure, configurable URL) | Should-have | S | `SPEC-schedule-notifications` |

## Timeline & Phases

| Phase | Features | Description |
|-------|----------|-------------|
| Phase 1 -- Core (this week) | F-01 | Scheduler engine with YAML config, retry logic, and SQLite persistence. Countries auto-sync on schedule. |
| Phase 2 -- API & Observability | F-02, F-03 | REST API for runtime schedule management, status monitoring, and webhook failure notifications. |

## Open Questions

1. What is the default cron schedule per country? **Proposed default**: Daily at 02:00 local time, staggered 15 min apart (Bolivia 02:00 BOT, Chile 02:15 CLT, Colombia 02:30 COT, etc.). Override per country in `schedules.yaml` as needed.
2. What webhook format should notifications use? (Slack-compatible JSON, generic HTTP POST, or configurable template?)
