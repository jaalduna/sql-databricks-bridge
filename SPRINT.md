# Sprint 5 - SQL-Databricks-Bridge: Calibration State Persistence

**Started**: 2026-02-19
**Status**: completed
**Goal**: Persist calibration job state across page navigation so running/completed jobs remain visible when returning to the Calibration page

## Backlog

| ID | Story | Priority | Status | Assignee | Notes |
|----|-------|----------|--------|----------|-------|
| S5-001 | Create CalibrationContext to hold activeJobIds map | high | done | frontend-dev | React Context with Record<country, jobId> |
| S5-002 | Refactor useCalibration to use context instead of useState | high | done | frontend-dev | Replace useState with context read/write |
| S5-003 | Provide CalibrationContext in app layout | high | done | frontend-dev | Wrapped AuthenticatedLayout |
| S5-004 | Verify with Playwright: navigate away and back | high | done | pancha | Argentina running + Bolivia completed both persisted |

## Architecture Decisions

- Use React Context (not Zustand) — matches existing AuthContext pattern, zero new deps
- Context holds `Record<string, string | null>` mapping country code to active job ID
- Provided at AuthenticatedLayout level (above Outlet, survives route changes)
- useCalibration hook API unchanged — CountryCard needs no changes

## Files Created/Changed

| File | Change |
|------|--------|
| `frontend/src/contexts/CalibrationContext.tsx` | New — context with getJobId/setJobId/clearJobId |
| `frontend/src/hooks/useCalibration.ts` | Replaced useState with context read/write |
| `frontend/src/components/AuthenticatedLayout.tsx` | Wrapped with CalibrationProvider |

## Daily Log

### 2026-02-19
- PO reported: calibration states lost when navigating between pages
- Root cause: activeJobId in component-local useState, destroyed on unmount
- Solution: CalibrationContext provided at layout level, survives route changes
- Playwright verified: triggered Argentina, navigated to Historial and back, running state persisted
