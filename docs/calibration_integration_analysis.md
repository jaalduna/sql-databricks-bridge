# Calibration Integration Analysis

**Date**: 2026-03-04
**Author**: Architecture Team
**Status**: Draft

---

## 1. Pipeline Mapping — Bridge Steps vs KWP Pipeline

### 1.1 Bridge's 6-Step State Machine

The bridge defines calibration as a 6-step pipeline in `src/sql_databricks_bridge/models/calibration.py`:

```python
CalibrationStepName = Literal[
    "sync_data",              # SQL Server -> Databricks (bridge handles directly)
    "copy_to_calibration",    # Bronze Copy job
    "merge_data",             # \
    "simulate_kpis",          #  > One Databricks job covers all 3
    "calculate_penetration",  # /
    "download_csv",           # Auto-completed (no Databricks job)
]
```

### 1.2 KWP Job Structures (per country)

| Country | Job Name Pattern | Tasks | Parameters |
|---------|-----------------|-------|------------|
| **Bolivia** | `Bolivia Penetration Calibration` | 3 tasks: `merge_data` -> `simulate_kpis` -> `calculate_penetration` | `start_period`, `final_period` |
| **Brasil** | `Brasil Penetration Calibration` | 1 task: `run_penet_calibration` | `catalog`, `start_period`, `final_period` |
| **CAM** | `Cam Penetration Calibration` | 1 task: `run_penet_calibration` | `start_period`, `final_period`, `group_by` |
| **Chile** | `Chile Penetration Calibration` | 1 task: `run_penet_calibration` | `start_period`, `final_period` |
| **Colombia** | `Colombia Penetration Calibration` | 1 task: `run_penet_calibration` | `start_period`, `final_period` |
| **Ecuador** | `Ecuador Penetration Calibration` | 1 task: `run_penet_calibration` | `start_period`, `final_period` |
| **Mexico** | `Mexico Penetration Calibration` | 1 task: `run_penet_calibration` | `start_period`, `final_period` |
| **Peru** | `Peru Penetration Calibration` | 1 task: `run_penet_calibration` | `start_period`, `final_period` |
| **Bronze Copy** | `Bronze Copy` | 1 task: `copy_bronze_tables` | `country`, `source_catalog`, `target_catalog` |

Volume calibration exists for Brasil and Mexico only (separate jobs or chained full-calibration jobs).

### 1.3 Bridge Job Name Resolution

The bridge uses `CalibrationJobLauncher._resolve_job_name()` with the template `"{Country} Penetration Calibration"`:

```python
# calibration_launcher.py line 69
"merge_data": StepJobSpec(
    job_name="{Country} Penetration Calibration",
    parameters={"start_period": "202102", "final_period": "0"},
    covers_steps=["merge_data", "simulate_kpis", "calculate_penetration"],
    task_step_map={
        "merge_data": "merge_data",
        "simulate_kpis": "simulate_kpis",
        "calculate_penetration": "calculate_penetration",
    },
)
```

The resolution logic (`line 182`):
```python
name = template.replace("{country}", country).replace("{Country}", country.capitalize())
```

**Problem**: The actual DAB job names are `"[dev] Chile Penetration Calibration"` (with bundle target prefix), but `_find_job_id()` searches by name via `self._client.find_job_by_name(resolved_name)`. The `job_name_prefix` constructor parameter could handle the `[dev]` prefix, but it must be set correctly at startup.

### 1.4 Parameter Mapping Analysis

| Bridge Parameter | Source | KWP Job Parameter | Status |
|-----------------|--------|-------------------|--------|
| `start_period` | Hardcoded `"202102"` in `DEFAULT_STEP_JOBS` | `start_period` | **WRONG** — hardcoded to 202102, most countries default to 202301. Should come from frontend or country config |
| `final_period` | `extra_params["final_period"]` from `TriggerRequest.period` | `final_period` | **OK** — passed via `_launch_calibration_step()` when period is set |
| `country` | Resolved from `TriggerRequest.country` | N/A (embedded in job name) | **OK** for job lookup; not passed as param to penet jobs |
| `source_catalog` | Hardcoded `"000-sql-databricks-bridge"` | `source_catalog` | **OK** |
| `target_catalog` | Hardcoded `"001-calibration-3-0"` | `target_catalog` | **OK** |
| `group_by` | **NOT PASSED** | `group_by` (CAM only) | **MISSING** — CAM needs `group_by` param, bridge doesn't send it |
| `catalog` | **NOT PASSED** | `catalog` (Brasil only) | **MISSING** — Brasil needs `catalog` param, defaults to `001-calibration-3-0` in YAML |
| `aggregations.region` | Passed as `extra_params["region"] = "true"` | N/A | **UNUSED** — KWP jobs don't read `region` param. This was bridge UI concept, not consumed by KWP |
| `aggregations.nivel_2` | Passed as `extra_params["nivel_2"] = "true"` | N/A | **UNUSED** — same issue |

### 1.5 Task-Step Mapping (Bolivia vs Others)

The bridge's `task_step_map` maps Databricks task keys to calibration steps:

```python
task_step_map={
    "merge_data": "merge_data",
    "simulate_kpis": "simulate_kpis",
    "calculate_penetration": "calculate_penetration",
}
```

**This only works for Bolivia**, which has 3 separate tasks with matching keys. All other countries have a single task `run_penet_calibration` which does NOT appear in the `task_step_map`.

**Consequence**: For non-Bolivia countries, `_track_task_steps()` finds no matching tasks, so per-step progress doesn't update. The monitor falls back to auto-completing all 3 covered steps at once when the single-task run terminates. This works functionally but provides no granular progress.

### 1.6 Step Flow Comparison

```
BRIDGE STATE MACHINE:                    KWP ACTUAL PIPELINE:
====================                     ====================

1. sync_data                             SQL Server -> Databricks Delta
   (bridge extracts SQL -> Delta)        (bridge handles this correctly)
        |
2. copy_to_calibration                   Bronze Copy notebook
   (Bronze Copy job)                     (copies 000-bridge -> 001-calibration)
        |
3. merge_data  ─────┐                   spark_pipeline.run_pipeline():
4. simulate_kpis     │ One Databricks     - extract_and_merge()    [merge]
5. calculate_penet.──┘ job (penet)        - run_simulation_and_collect() [simulate]
        |                                  - postprocess_kpi_results()
6. download_csv                          penet_pipeline.run_penet_targets():
   (auto-complete)                         - Prophet forecasting  [penetration]
                                           - save to Delta
```

**Gaps**:
- The bridge has no concept of **volume calibration** — only penetration is modeled
- The bridge's `download_csv` step auto-completes without doing anything. The actual download happens via `GET /events/{job_id}/download` which reads from Delta tables
- No step for **Excel report generation** (currently a manual script)

### 1.7 Wheel Version Sync

All KWP job YAMLs reference:
```yaml
- whl: ${workspace.artifact_path}/.internal/kwp_intelligence_calibration-0.3.43-py3-none-any.whl
- whl: ${workspace.artifact_path}/.internal/kwp_simulators-0.8.14-py3-none-any.whl
```

The bridge has **no awareness of wheel versions**. It triggers jobs by name, and the jobs use whatever wheel is deployed. The critical deployment rule (from CLAUDE.md) is: *bump version in ALL job YAMLs when deploying, because the cluster caches wheels by filename.*

**Risk**: If someone deploys a new KWP wheel via `databricks bundle deploy` but the bridge user triggers a job before that, the job uses the old code. The bridge should ideally show the deployed wheel version in the UI.

---

## 2. Excel Report Generation Design

### 2.1 Current State

The script `scripts/csv_targets_to_excel.py` (232 lines):
- Reads a CSV (penet_targets from Gold Delta table)
- If `Pais` column exists (CAM multi-country), splits into one sheet per country
- Otherwise creates a single data sheet
- Adds an "Introduccion" sheet with methodology, column definitions, and sheet index
- Uses openpyxl for styling (Calibri fonts, colored headers, alternating rows)

### 2.2 Proposed Backend Endpoint

```
POST /api/calibration/{job_id}/report/excel
```

**Request body** (optional overrides):
```json
{
    "table": "001-calibration-3-0.gold_cam.penet_targets",
    "format": "targets_excel"
}
```

**Response**: Streaming XLSX download (`application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`)

**Implementation strategy**:

1. **Refactor `csv_targets_to_excel.py` into a reusable module** at `src/sql_databricks_bridge/core/excel_report.py`:
   ```python
   def generate_penet_targets_excel(df: pd.DataFrame, country: str) -> bytes:
       """Generate Excel report from penet_targets DataFrame.
       Returns XLSX as bytes buffer."""
   ```

2. **New endpoint** in `src/sql_databricks_bridge/api/routes/trigger.py` (or a new `reports.py` router):
   ```python
   @router.get("/events/{job_id}/report/excel")
   async def download_excel_report(
       job_id: str,
       user: CurrentAzureADUser,
       raw_request: Request,
       table: str | None = Query(default=None),
   ) -> StreamingResponse:
       # 1. Get job detail, determine country
       # 2. Read penet_targets from gold_{country} Delta table
       # 3. Call generate_penet_targets_excel(df, country)
       # 4. Return StreamingResponse with XLSX content-type
   ```

3. **Table auto-detection**: If no `table` param, auto-select `001-calibration-3-0.gold_{country}.penet_targets`

4. **Dependencies**: Add `openpyxl` to the bridge's `pyproject.toml` (it's already an indirect dep via pandas)

### 2.3 Frontend Integration

Add an "Excel Report" button to the `CalibrationDetailModal` (or job detail view):

```tsx
// In CalibrationDetailModal, next to existing CSV download button
<Button
  variant="outline"
  disabled={!isCompleted}
  onClick={() => downloadExcelReport(jobId)}
>
  <FileSpreadsheet className="mr-2 h-4 w-4" />
  Download Excel Report
</Button>
```

The download function:
```typescript
async function downloadExcelReport(jobId: string) {
  const response = await fetch(`/api/events/${jobId}/report/excel`);
  const blob = await response.blob();
  // Trigger browser download
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `calibration_report_${jobId.slice(0, 8)}.xlsx`;
  a.click();
}
```

### 2.4 Complexity: Low-Medium

- Refactoring the existing script is straightforward (it's already well-structured)
- The bridge already has CSV streaming (`download_event_csv`), so the pattern is established
- openpyxl is a pure-Python dependency, no system packages needed

---

## 3. Validation/Analysis Feature Design

### 3.1 Current State (Jupyter Notebook)

The notebook `notebooks/cam_calibration_v042_report.ipynb` performs:

1. **Data loading**: Read 4 CSVs from Gold Delta tables (penet_targets, kpis_all, kpis_original, kpis_bc) + reference `.xlsm` files
2. **Overview statistics**: Per-country diff summary (|Diff| <= 2pp, 5pp, 10pp counts, mean, max)
3. **Scatter plots**: Target Actual vs Target 3.0 per country (color-coded by diff magnitude)
4. **Histograms**: Distribution of differences (pp)
5. **yhat_kl origin distribution**: Stacked bar chart showing BC adjustment types
6. **Time series**: Historical PENET + Target Actual + Target 3.0 with Prophet CI
7. **Deep dive**: Top products by largest diff, with time series
8. **CI analysis**: Prophet CI width vs target deviation, Target Actual within CI percentage

### 3.2 Proposed Validation API

#### Endpoint 1: Summary Statistics

```
GET /api/calibration/{job_id}/validation/summary
```

**Response**:
```json
{
  "country": "cam",
  "period": "202602",
  "countries": [
    {
      "name": "Costa Rica",
      "products": 115,
      "within_2pp": 94,
      "within_5pp": 108,
      "within_10pp": 114,
      "max_positive_diff": 0.066,
      "max_negative_diff": -0.105,
      "mean_abs_diff": 0.014
    }
  ],
  "origin_distribution": {
    "Costa Rica": {
      "BC_ADJUSTED_KPI_WITHIN_HISTORICAL_BAND": 84,
      "BC_ADJUSTED_KPI_CLOSE_0": 27
    }
  }
}
```

**Server-side computation**:
- Read `gold_{country}.penet_targets` from Delta
- Read reference targets from a stored baseline (see 3.4 below)
- Compute diffs and summary statistics using Polars (in-memory, ~1K rows)

#### Endpoint 2: Time Series Data

```
GET /api/calibration/{job_id}/validation/timeseries?product={id}&country={name}
```

**Response**:
```json
{
  "product": "14",
  "country": "Costa Rica",
  "history": [
    {"period": 202102, "penet": 0.78},
    {"period": 202103, "penet": 0.76}
  ],
  "target_actual": 0.785,
  "target_30": {
    "yhat": 0.79,
    "yhat_lower": 0.68,
    "yhat_upper": 0.90,
    "yhat_kl": 0.792,
    "yhat_kl_origin": "BC_ADJUSTED_KPI_WITHIN_HISTORICAL_BAND"
  }
}
```

**Server-side computation**:
- Read `gold_{country}.kpis_all` filtered by product+country
- Read `gold_{country}.penet_targets` for target values
- Reference baseline from stored `.xlsm` or Delta table

#### Endpoint 3: Product Comparison Table

```
GET /api/calibration/{job_id}/validation/products?country={name}&sort_by=abs_diff&limit=20
```

Returns paginated product-level comparison: Target Actual, Target 3.0, diff, origin, CI width.

### 3.3 Frontend Charts

Using a charting library (Recharts is already in the bridge frontend):

1. **Summary dashboard** (visible in CalibrationDetailModal after completion):
   - Per-country summary cards (products count, % within 5pp)
   - Stacked bar chart of yhat_kl origins

2. **Product drill-down** (expandable section or new page):
   - Scatter plot: Target Actual vs Target 3.0
   - Time series chart with historical PENET + forecast CI band
   - Sortable/filterable product table

3. **No .xlsm reference needed**: The baseline comparison requires a reference dataset. Options:
   - Store previous calibration run's targets in a `baseline_targets` Delta table
   - Upload .xlsm files via bridge UI (one-time setup per country)
   - Compare current run vs previous run (version N vs N-1)

### 3.4 Reference Baseline Strategy

The .xlsm files are currently loaded manually per-country. For the bridge:

**Recommended approach**: Store baselines in Delta

```sql
CREATE TABLE `001-calibration-3-0`.calibration_baselines (
    country STRING,
    product_id STRING,
    product_name STRING,
    target_actual DOUBLE,
    penet_actual DOUBLE,
    baseline_version STRING,
    uploaded_at TIMESTAMP
)
```

- One-time upload from .xlsm files (admin endpoint or manual ETL)
- Subsequent comparisons use the Gold table from the previous calibration run
- Validation compares: `gold_{country}.penet_targets.yhat_kl` vs `calibration_baselines.target_actual`

### 3.5 Complexity: Medium-High

- Summary endpoint: Medium (Delta queries + in-memory stats)
- Time series endpoint: Medium (scoped query, small data)
- Product comparison: Low (pagination + sorting)
- Frontend charts: Medium (Recharts components)
- Baseline management: Medium (upload/store workflow)

---

## 4. Implementation Priority

### Phase 1: Fix Parameter Mapping (Critical, Low effort)

1. **Fix `start_period` hardcoding** — read from country config or frontend input instead of `"202102"`
2. **Add `group_by` parameter** for CAM — either via frontend aggregation options or per-country config in the launcher
3. **Verify job name resolution** — ensure `_find_job_id()` works with the `[dev]` prefix from DAB

**Files to change**:
- `src/sql_databricks_bridge/core/calibration_launcher.py` — `DEFAULT_STEP_JOBS`, add per-country overrides
- `src/sql_databricks_bridge/api/routes/trigger.py` — pass `start_period` from frontend or config

### Phase 2: Excel Report Download (Medium effort)

1. Refactor `csv_targets_to_excel.py` into bridge-importable module
2. Add `GET /events/{job_id}/report/excel` endpoint
3. Add "Download Excel" button to frontend

**Files to create/modify**:
- `src/sql_databricks_bridge/core/excel_report.py` (new)
- `src/sql_databricks_bridge/api/routes/trigger.py` (add endpoint)
- `calibration_frontend/src/components/CalibrationDetailModal.tsx` (add button)

### Phase 3: Validation Summary (Medium effort)

1. Add `GET /calibration/{job_id}/validation/summary` endpoint
2. Add summary cards to CalibrationDetailModal
3. Add baseline management (upload or Delta storage)

### Phase 4: Full Validation UI (High effort)

1. Time series and product comparison endpoints
2. Interactive charts (scatter, time series, histograms)
3. Product drill-down page

### Phase 5: Volume Calibration Support (High effort)

1. Add volume calibration steps to the state machine
2. New step names: `simulate_volume`, `calculate_volume`
3. Job launcher mapping for volume-specific Databricks jobs
4. Frontend UI for selecting penet vs vol vs full calibration

---

## 5. Risks and Open Questions

### 5.1 Job Name Resolution
**Risk**: The bridge's `_resolve_job_name()` uses `country.capitalize()` to produce `"{Country} Penetration Calibration"`. But DAB job names are `"[dev] {Country} Penetration Calibration"`. If `job_name_prefix` is not set correctly at startup, job lookups will fail silently (returns None, step marked as error).

**Mitigation**: Verify `job_name_prefix` is set to `"[dev] "` (or `"[prod] "` for production). Add a startup health check that verifies all expected job names resolve.

### 5.2 Wheel Version Drift
**Risk**: The bridge has no mechanism to verify which wheel version is deployed. A user could trigger a calibration that runs stale code if wheels weren't bumped.

**Mitigation**: Add a `/health/wheel-versions` endpoint that reads deployed wheel filenames from the workspace. Display in the calibration UI.

### 5.3 Single-Task Countries Get No Granular Progress
**Risk**: 7 of 8 countries use a single `run_penet_calibration` task. The bridge's task_step_map only matches Bolivia's 3-task structure. Other countries show merge_data -> simulate_kpis -> calculate_penetration all completing at once.

**Options**:
- (A) Migrate all countries to Bolivia's 3-task pattern (high effort, but best UX)
- (B) Accept coarse progress for single-task countries (low effort)
- (C) Add log-based progress parsing (medium effort, fragile)

**Recommendation**: Option B short-term, Option A as a follow-up for countries where run time > 10min.

### 5.4 Missing Volume Pipeline Support
The bridge only models penetration calibration. Brasil and Mexico also have volume jobs (`{Country} Volume Calibration`) and full calibration jobs (`{Country} Full Calibration`). The bridge cannot trigger or monitor these.

### 5.5 Aggregation Options Are Not Consumed
The bridge passes `aggregations.region` and `aggregations.nivel_2` as job parameters, but **no KWP job reads these**. CAM uses `--group-by` instead, and other countries use hardcoded `group_by_cols` in hooks.

**Recommendation**: Either:
- Map `aggregations` to `--group-by` in the launcher for CAM
- Or remove the aggregation UI and rely on the KWP YAML defaults

### 5.6 `start_period` Mismatch
The bridge hardcodes `start_period: "202102"` but:
- Most country YAMLs default to `"202301"`
- The bridge's `TriggerRequest` has no `start_period` field
- The notebook entry points default to 202301

This means the bridge sends `start_period=202102` (2 years more data than needed), increasing job runtime.

### 5.7 Auth / Service Principal
The bridge uses Azure AD tokens for Databricks API calls. The KWP pipeline uses the cluster's identity. If the bridge's SP doesn't have permissions on `001-calibration-3-0` catalog, job launches will work (via Jobs API) but Delta reads for CSV/Excel download will fail.

**Status**: Blocked on Francisco (Service Principal creation, per CLAUDE.md).

### 5.8 Performance for Validation Queries
Reading `kpis_all` from Gold Delta (~40K rows per country) via Statement Execution API is fast (<5s). But if the frontend makes per-product time series requests, this could become chatty.

**Mitigation**: Batch the kpis_all read once per validation session and cache server-side (LRU with 15-min TTL keyed by `(job_id, country)`).

---

## Appendix A: File Reference

### Bridge (sql-databricks-bridge)
| File | Purpose |
|------|---------|
| `src/sql_databricks_bridge/core/calibration_launcher.py` | Maps steps to Databricks jobs, resolves names/params |
| `src/sql_databricks_bridge/core/databricks_monitor.py` | Polls Databricks run status, advances steps |
| `src/sql_databricks_bridge/core/calibration_tracker.py` | In-memory step state management |
| `src/sql_databricks_bridge/models/calibration.py` | Step names, CalibrationStep model, 6-step order |
| `src/sql_databricks_bridge/api/routes/trigger.py` | Trigger endpoint, background extraction, CSV download |

### KWP (KWP-IntelligenceCalibration)
| File | Purpose |
|------|---------|
| `asset-bundles/resources/jobs/jobs_{country}_calibration.yml` | DAB job definitions per country |
| `asset-bundles/notebooks/{country}_penet_calibration.py` | Databricks entry points (argparse CLI) |
| `asset-bundles/notebooks/bronze_copy.py` | Bronze Copy entry point |
| `src/kwp_intelligence_calibration/pipeline/spark_pipeline.py` | Generic Spark pipeline (extract, merge, simulate) |
| `src/kwp_intelligence_calibration/pipeline/penet_pipeline.py` | Penetration targets (Prophet forecasting) |
| `src/kwp_intelligence_calibration/pipeline/vol_pipeline.py` | Volume targets |
| `scripts/csv_targets_to_excel.py` | Excel report generation (standalone script) |
| `notebooks/cam_calibration_v042_report.ipynb` | Validation analysis notebook (Plotly charts) |

### Wheel Versions (current)
- `kwp_intelligence_calibration-0.3.43-py3-none-any.whl`
- `kwp_simulators-0.8.14-py3-none-any.whl`
