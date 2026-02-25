# Sprint 9 - SQL-Databricks-Bridge: Fix "Calibrar" Silent Failure

**Started**: 2026-02-25
**Status**: completed
**Goal**: Fix calibration trigger flow so errors are visible and the card reflects actual state

## Bug Report (from PO)

When pressing "Calibrar" for Argentina (no steps skipped):
- Toast says "Calibration started from argentina"
- No sync process actually starts
- Frontend card doesn't change state

## Root Cause Analysis (Architect)

**Two bugs found:**

### Bug 1 (PRIMARY - Frontend): Optimistic toast fires before request completes
- **File**: `frontend/src/components/CountryCard.tsx` ~line 181
- `toast.success()` fires synchronously on click, **before** the POST resolves
- If POST fails (500, network error), user sees false "started" message
- Mutation errors are never displayed — `triggerError` is captured but never shown
- Card briefly shows "Starting..." then silently reverts to idle

### Bug 2 (CONTRIBUTING - Backend): Unprotected `insert_job` in non-skip-sync path
- **File**: `src/sql_databricks_bridge/api/routes/trigger.py` ~line 1179
- `insert_job()` to Databricks Delta table is NOT wrapped in try/except
- The skip-sync path (line ~1023-1038) correctly wraps this in try/except
- If Databricks is unreachable/token expired → unhandled 500 error
- This 500 is then silently swallowed by Bug 1

## Backlog

| ID | Story | Priority | Status | Assignee | Notes |
|----|-------|----------|--------|----------|-------|
| S9-001 | Move toast to onSuccess callback, add onError toast | high | done | frontend-dev | CountryCard.tsx + useCalibration.ts |
| S9-002 | Wrap insert_job in try/except (non-skip-sync path) | high | done | backend-dev | trigger.py ~line 1179 |
| S9-003 | Test frontend error handling | medium | done | frontend-tester | tsc clean, 28/28 pass (11 pre-existing failures unrelated) |
| S9-004 | Test backend insert_job resilience | medium | done | backend-tester | 555/555 pass (78 pre-existing failures unrelated) |
| S9-005 | Code review | medium | done | code-reviewer | APPROVED WITH NITS — job_id nit addressed |

## Architecture Decisions

- Toast success/error must reflect actual API response, not be optimistic
- Delta table insert_job is best-effort — job should proceed if Delta record fails (SQLite has it too)
- Match existing try/except pattern from skip-sync path

## Key Files Changed

| File | Change |
|------|--------|
| `src/sql_databricks_bridge/api/routes/trigger.py` | Wrapped `insert_job` in try/except with logger.warning including job_id |
| `frontend/src/hooks/useCalibration.ts` | Changed `trigger.mutate()` → `trigger.mutateAsync()` with `return` |
| `frontend/src/components/CountryCard.tsx` | Replaced immediate toast with `.then()` success / `.catch()` error |

## Daily Log

### 2026-02-25
- PO reported: Calibrar for Argentina shows success toast but nothing happens
- Architect traced full flow: button click → POST /trigger → insert_job → background task
- Found 2 bugs: optimistic toast (frontend) + unprotected insert_job (backend)
- Backend-dev and frontend-dev fixed both bugs in parallel
- Testers confirmed no new regressions (all failures pre-existing)
- Code reviewer: APPROVED WITH NITS — addressed job_id logging nit
