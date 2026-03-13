# Sprint 13 - SQL-Databricks-Bridge: Calibration Pipeline Integration & Validation

**Started**: 2026-03-04
**Status**: planning
**Goal**: Analyze how the KWP-IntelligenceCalibration pipeline maps to the sql-databricks-bridge frontend/backend, identify gaps, and plan implementation of Excel report generation + validation features in the bridge.

## Research Results

| ID | Story | Priority | Status | Assignee | Notes |
|----|-------|----------|--------|----------|-------|
| S13-001 | Architect: Pipeline mapping analysis | high | done | architect | 5 critical gaps found |
| S13-002 | Architect: Excel report generation design | high | done | architect | Straightforward refactor |
| S13-003 | Architect: Validation/analysis feature design | medium | done | architect | 3-tier approach |
| S13-004 | UX: Frontend calibration UX gap analysis | medium | done | ux-designer | 10 gaps identified |

## Architecture Decisions

See `docs/calibration_integration_analysis.md` for full analysis.
See `docs/calibration_ux_gaps.md` for UX gap analysis.

### Critical Gaps Found

1. `start_period` hardcoded to `"202102"` — should be `"202301"` or configurable
2. `group_by` not passed for CAM — CAM needs `--group-by` parameter
3. `task_step_map` only works for Bolivia (3-task job) — other countries get no granular progress
4. `aggregations.region` and `aggregations.nivel_2` are dead params — KWP doesn't read them
5. Volume calibration not supported — Brasil and Mexico need vol pipeline

### Proposed Implementation Phases

- **Phase 1**: Fix parameter mapping (critical, low effort)
- **Phase 2**: Excel report download (medium effort)
- **Phase 3**: Validation summary endpoint + UI (medium effort)
- **Phase 4**: Full validation UI with charts (high effort)
- **Phase 5**: Volume calibration support (high effort)

## Daily Log

### 2026-03-04
- PO requested analysis of KWP pipeline integration with sql-databricks-bridge
- Spawned architect + UX designer for parallel research
- Architect produced `docs/calibration_integration_analysis.md` (5 critical gaps, Excel/validation designs)
- UX designer produced `docs/calibration_ux_gaps.md` (10 gaps, 4 proposed components)
- Awaiting PO decision on which phases to prioritize
