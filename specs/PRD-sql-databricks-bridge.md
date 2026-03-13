# PRD: SQL-Databricks Bridge

**Slug**: `sql-databricks-bridge`
**Status**: draft
**Created**: 2026-03-12
**Author**: Joaquin Aldunate

---

## Vision

A unified bridge service that enables seamless bidirectional data flow between on-premise SQL Servers and Databricks Unity Catalog, providing data engineers and analysts with a single control plane to sync data, trigger calibration/data-quality jobs, and monitor pipeline health — all without requiring direct network access from Databricks to on-premise systems.

When this initiative is complete, country analysts can trigger and monitor their own data quality workflows from a desktop app, while data engineers maintain full control over sync configurations, job definitions, and data freshness guarantees.

## Problem Statement

Databricks cannot access on-premise SQL Server instances due to network restrictions. This is a known temporary limitation (estimated 6-12 months to resolve), but the organization needs to continue operating multi-country calibration and data quality pipelines that require data from both environments.

**Today's pain points:**
- No automated path to move reference/dimension data from SQL Server into Databricks catalogs
- No way for Databricks jobs to write results back to SQL Server tables
- Analysts depend on engineers to manually move data or trigger pipelines
- 8 countries (Argentina, Bolivia, Brasil, Chile, Colombia, Ecuador, Mexico, Peru) each with their own SQL Server instance, multiplying the coordination overhead

## Goals & Success Metrics

| Goal | Metric | Target |
|------|--------|--------|
| Data freshness | Time between SQL Server change and Databricks table update | < 30 minutes for differential sync |
| Sync reliability | Extraction + upload success rate across all countries | > 99% per scheduled run |
| Analyst autonomy | % of calibration runs triggered by analysts (not engineers) | > 80% within 3 months of frontend launch |
| Operational visibility | Mean time to detect a failed sync/job | < 5 minutes (via dashboard + alerts) |

## Target Users

| User Type | Description | Primary Need |
|-----------|-------------|-------------|
| Data Engineer | Builds and maintains calibration pipelines, SQL queries, Databricks jobs | Configure sync, deploy job wheels, debug failures |
| Country Analyst | Runs calibration workflows for their assigned country | Trigger pipelines, monitor progress, download results |

## User Stories

> Format: As a [user type], I want [action] so that [benefit].

1. **US-01**: As a data engineer, I want to define SQL queries per country that extract reference data so that I can sync specific tables to Databricks without manual intervention.
2. **US-02**: As a data engineer, I want differential sync (only changed rows) so that extraction is fast and doesn't re-upload unchanged data.
3. **US-03**: As a data engineer, I want Databricks jobs to write events back to SQL Server so that calibration results are available on-premise.
4. **US-04**: As a country analyst, I want to trigger a calibration pipeline from the desktop app so that I don't need to ask engineering for every run.
5. **US-05**: As a country analyst, I want to see pipeline progress in real-time so that I know when results are ready or if something failed.
6. **US-06**: As a data engineer, I want country-specific query overrides so that each country's SQL Server schema differences are handled transparently.
7. **US-07**: As a country analyst, I want to check panel eligibility status per country so that I can validate data quality before running calibration.
8. **US-08**: As a data engineer, I want the bridge to be deployable as a background service on a Windows machine so that sync runs continuously without manual restarts.

## Proposed Solution

A FastAPI backend running on-premise (where it has network access to both SQL Server and Databricks) that acts as the single API gateway. A React + Tauri desktop frontend provides the analyst-facing control plane.

### Key Capabilities

1. **CAP-01**: **SQL Server Extraction** — Execute country-specific SQL queries, load results into Polars DataFrames, and upload to Databricks Delta tables via stage-then-CTAS.
2. **CAP-02**: **Differential Sync** — 2-level fingerprint comparison (group-level then row-level) to extract only changed data slices.
3. **CAP-03**: **Databricks-to-SQL Event Sync** — Poll a `bridge_events` Delta table for write-back events, execute INSERT/UPDATE/DELETE on SQL Server.
4. **CAP-04**: **Calibration Pipeline Orchestration** — Trigger and monitor 6-step Databricks Asset Bundle jobs (sync → copy_bronze → merge → simulate_weights → simulate_kpis → calculate_targets).
5. **CAP-05**: **Country-Aware Query Resolution** — Priority-based query loading: country-specific overrides > common fallback.
6. **CAP-06**: **Desktop Frontend** — React + Tauri app with dashboard, pipeline triggering, job monitoring, calibration history, and eligibility views.
7. **CAP-07**: **Auth & Permissions** — Token-based auth with Azure AD integration, role-based permissions per country/action.

## Scope

### In Scope
- Bidirectional data sync between SQL Server and Databricks
- REST API for all bridge operations
- Calibration pipeline triggering and monitoring
- Panel eligibility management
- Desktop app (Tauri) for analysts
- Multi-country support (8 countries)
- Differential sync with fingerprinting
- Auth via Azure AD + token

### Out of Scope
- Direct Databricks-to-SQL-Server connectivity (that's the infra team's problem)
- Business logic inside calibration/MTR/merge pipelines (those are separate repos deployed as wheels)
- Automated scheduling (APScheduler integration is a future consideration)
- CI/CD pipelines (tracked separately)

### Future Considerations
- **Sunset plan**: When Databricks gains direct SQL Server access, the extraction/sync layer becomes unnecessary. The frontend + pipeline orchestration may survive as a standalone tool.
- **Periodic scheduler**: APScheduler or similar for per-country timezone-aware automatic sync.
- **Multi-tenant**: Supporting multiple Databricks workspaces.

## System Context

```
                     On-Premise Network                              Cloud (Azure)
                    ┌─────────────────────┐                    ┌──────────────────────┐
                    │                     │                    │    Databricks        │
 ┌──────────┐      │  ┌───────────────┐  │    HTTPS/REST      │  ┌───────────────┐   │
 │ SQL      │◄────►│  │   Bridge      │  │◄──────────────────►│  │ Unity Catalog │   │
 │ Server   │      │  │   Backend     │  │                    │  │ (Delta Tables)│   │
 │ (per     │      │  │   (FastAPI)   │  │                    │  └───────────────┘   │
 │  country)│      │  └───────┬───────┘  │                    │  ┌───────────────┐   │
 └──────────┘      │          │          │                    │  │ DAB Jobs      │   │
                    │          │          │                    │  │ (wheels from  │   │
                    │  ┌───────┴───────┐  │                    │  │  other repos) │   │
                    │  │   Frontend    │  │                    │  └───────────────┘   │
                    │  │   (Tauri App) │  │                    │                      │
                    │  └───────────────┘  │                    └──────────────────────┘
                    └─────────────────────┘
```

**Data flow — SQL Server → Databricks (Extraction):**
`SQLServerClient` → `Extractor` (Polars) → `DeltaWriter` (stage-then-CTAS to Delta tables)

**Data flow — Databricks → SQL Server (Event Sync):**
Databricks job writes to `bridge_events` table → `EventPoller` detects → `SyncOperator` executes on SQL Server

**Job orchestration:**
Frontend → Bridge API → `CalibrationJobLauncher` → Databricks runs job (wheel from external repo) → `DatabricksJobMonitor` polls status → WebSocket/API updates frontend

## Risks & Mitigations

| Risk | Impact | Likelihood | Mitigation |
|------|--------|-----------|------------|
| Network limitation resolved later than expected (>12 months) | High | Medium | Design for maintainability; invest in reliability, not just "good enough" |
| SQL Server schema differences across countries cause extraction failures | Medium | High | Country-specific query overrides + validation in extraction step |
| Databricks token expiration causes silent sync failures | High | Medium | Health checks, token refresh, alerting on consecutive failures |
| Large tables overwhelm differential sync | Medium | Low | 2-level fingerprinting already handles this; monitor perf on largest country (Brasil) |
| Frontend scope creep as more processes are added | Medium | High | Keep frontend focused on bridge operations; other repos own their own orchestration logic via job wheels |
| Wheel version caching on Databricks cluster | High | High | Version bump in pyproject.toml + update all job YMLs on every deploy |

## Assumptions

- The on-premise machine running the bridge has stable network access to both SQL Server instances and Databricks (outbound HTTPS).
- Each country's SQL Server schema is stable enough that query overrides cover differences (no frequent schema changes).
- Databricks Asset Bundle jobs are the standard deployment mechanism for all data quality processes.
- The 6-12 month timeline for direct Databricks→SQL Server access is a rough estimate, not a commitment.
- `kantar_db_handler` (vendored wheel) will continue to resolve country-specific SQL Server connections.

## Dependencies

- **Databricks workspace**: `adb-2125673683576002.2.azuredatabricks.net` (shared cluster `0119-144658-rpqzfqu1`)
- **External repos (deployed as wheels/jobs)**:
  - `kwp-intelligence-calibration` — calibration pipeline logic
  - `mtr/` — MTR calibration
  - `kwp-data-merge` — data merge pipeline
- **`kantar_db_handler`** — vendored wheel for SQL Server connection resolution
- **Azure AD** — authentication for frontend users
- **Service Principal** — for CI/CD and automated Databricks access (configuration pending)

## Feature Breakdown

> These are candidate features that will each become a separate spec (via `/spec`).

| # | Feature | Priority | Complexity | Spec |
|---|---------|----------|-----------|------|
| F-01 | SQL Server extraction with country-aware queries | Must-have | M | — |
| F-02 | Delta table writing (stage-then-CTAS) | Must-have | M | — |
| F-03 | Differential sync (2-level fingerprinting) | Must-have | L | — |
| F-04 | Databricks-to-SQL Server event sync (poller + operations) | Must-have | L | — |
| F-05 | Calibration pipeline orchestration (trigger + monitor) | Must-have | L | — |
| F-06 | Panel eligibility management | Should-have | M | — |
| F-07 | Desktop frontend (Tauri + React dashboard) | Should-have | XL | — |
| F-08 | Auth & permissions (Azure AD + token + roles) | Must-have | M | — |
| F-09 | Calibration history & CSV download | Should-have | S | — |
| F-10 | Real-time job monitoring (WebSocket/polling) | Should-have | M | — |
| F-11 | Automated periodic scheduler | Nice-to-have | M | — |
| F-12 | CI/CD pipeline (Azure DevOps + Service Principal) | Should-have | M | — |

## Timeline & Phases

| Phase | Features | Description |
|-------|----------|-------------|
| Phase 1 — Core Sync (Done) | F-01, F-02, F-03, F-04, F-08 | Bidirectional data sync with auth — already implemented |
| Phase 2 — Pipeline Orchestration (Done) | F-05, F-10 | Calibration job triggering and monitoring — already implemented |
| Phase 3 — Analyst UX (In Progress) | F-06, F-07, F-09 | Desktop app, eligibility, calibration history — partially implemented |
| Phase 4 — Production Hardening | F-11, F-12 | Scheduler, CI/CD, Service Principal — pending |

## Open Questions

1. **Sunset strategy**: When Databricks gains SQL Server access, which components survive? The pipeline orchestration frontend likely has value beyond the sync use case.
2. **Scheduler priority**: Should automated periodic sync (F-11) be prioritized before CI/CD (F-12), or vice versa?
3. **Frontend packaging**: Should the Tauri desktop app be the primary distribution, or should a web deployment also be maintained?
4. **Multi-workspace**: Will there ever be more than one Databricks workspace to support?
