# Calibration UX Gap Analysis

> Reviewed: 2026-03-04
> Scope: `frontend/src/pages/CalibrationPage.tsx` and all supporting components/hooks

---

## 1. Current UX Flow Description

### Entry Point: CalibrationPage

The page renders a **grid of CountryCards** (one per country) alongside a global **PeriodSelector** and a **CalibrationConfigDialog** (gear icon). The period defaults to the previous calendar month.

### CountryCard

Each card shows:
- Country flag + name
- Two badges: last sync time and last calibration time (relative: "3h ago", "never")
- Data availability pills: Elegibilidad (green/grey) and Pesaje (green/grey)
- A **"Calibrar" button** — disabled unless Elegibilidad = true and no job is running
- A **stop button** (appears when running)
- An **eye icon button** to open CalibrationDetailModal (appears when a job exists in local session state)
- A **CalibrationProgress** bar with 6 step-dots when a job is active
- Status badges (Completed, Failed, Cancelled)

Clicking the card body (outside buttons) opens the **CalibrationHistoryModal**.

### Confirmation Dialog (inline AlertDialog)

Before firing the job, users see a confirm dialog that summarizes:
- Country + period
- Active aggregations (Region / Nivel 2) if any
- Overrides (row limit, lookback months) if any
- Two optional checkboxes: "Skip data sync" and "Skip data copy"

### CalibrationProgress

During a run, a progress bar + 6 step dots appear on the card. Labels update to show the current step (and the running Databricks task name within a step). `sync_data` shows fractional progress based on query count.

### CalibrationDetailModal

Opened via the eye icon (only visible when a job exists in the current session). Shows:
- Country, Tag, Started, Completed, Duration, Queries count
- Collapsible step rows (6 steps) with:
  - Status icon + duration
  - Sub-tasks (Databricks task keys) with status + duration + errors
  - Query-level results (rows extracted, duration, errors) under `sync_data`
- A global error message if the job-level error is set
- A **"Download CSV"** button at the bottom

### CalibrationHistoryModal

Opened by clicking the card body. Lists up to 20 past runs for the country. Each entry shows:
- Date/time of run
- Period (YYYYMM)
- Total rows extracted
- Status badge
- Duration
- Download button (CSV only, disabled for non-completed runs)

When the download button is clicked, a **TablePicker** appears inline below the row. If only one table exists, it auto-downloads. If multiple tables exist, a dropdown lets the user select which Delta table to download as CSV.

### CalibrationConfigDialog

Accessible via the gear icon. Controls **applied globally** to all countries:
- Aggregations: Region checkbox, Nivel 2 checkbox
- Overrides: row_limit (number input), lookback_months (number input)
- No save/reset buttons — changes are live (controlled state in CalibrationPage)

---

## 2. Gap Analysis

### GAP-1: No post-calibration report generation [Priority: HIGH]

**What's missing**: After calibration completes, users must manually open a terminal, run `scripts/csv_targets_to_excel.py`, and generate Jupyter notebooks. There is no in-app path to trigger Excel report generation or download a `.xlsx` file.

**Impact**: The calibration workflow is incomplete inside the app. The "Download CSV" button provides raw data but no analyst-ready report.

**Proposed fix**: See Section 3.1 — Add an "Export Report" action to the detail modal and history entries that triggers an Excel report generation job and provides a download link when ready.

---

### GAP-2: No validation / KPI comparison view [Priority: HIGH]

**What's missing**: There is no way to see whether the calibrated KPIs are reasonable. Users cannot compare the current run's KPI results (PENET, VOL, TRANS) against previous runs or against targets. Validation logic exists in the backend (scripts), but no UI surface exposes it.

**Impact**: Users must download CSVs and run external analysis to validate results, making the app only useful for job triggering, not for decision-making.

**Proposed fix**: See Section 3.2 — Add a KPI Summary panel inside CalibrationDetailModal (or a new CalibrationResultsPage) that shows aggregated KPIs per country/period with sparkline trend vs previous run.

---

### GAP-3: Eye icon only visible in current session [Priority: HIGH]

**What's missing**: The "View details" eye icon (which opens CalibrationDetailModal) only renders when `job` is set — meaning only for the job triggered in the **current browser session**. After a page refresh, or when viewing a different browser/user session, the eye icon disappears for completed jobs.

**Impact**: Historical jobs visible in the History modal show no way to open the full step-level detail view. Users lose access to detailed task-level diagnostics after navigating away.

**Proposed fix**: Add an eye/detail button to each row in CalibrationHistoryModal, fetching `EventDetail` on demand (the API already supports `GET /events/{job_id}`).

---

### GAP-4: Config is global but pipelines are per-country [Priority: MEDIUM]

**What's missing**: The CalibrationConfigDialog sets aggregations, row_limit, and lookback_months for **all countries simultaneously**. Countries have different calibration requirements (Ecuador runs 1D, Brasil has Buen Colaborador logic, CAM has paiscam-level overrides).

**Impact**: Power users doing multi-country calibrations with different settings must either run one at a time and change config between runs, or accept the same config for all.

**Proposed fix**: Add per-country config overrides as a secondary setting accessible from the confirm dialog. Show a "Customize" link in the AlertDialog that expands per-country fields.

---

### GAP-5: No aggregate status view across all countries [Priority: MEDIUM]

**What's missing**: The card grid shows individual country status, but there is no summary row or dashboard section showing:
- How many countries have been calibrated for the selected period
- Which countries have stale data (calibrated more than X days ago for this period)
- An overall "Run All" action to queue all eligible countries

**Impact**: Users managing 8 countries must visually scan each card to understand pipeline health. Missing a country is easy.

**Proposed fix**: See Section 3.3 — Add a summary header bar above the grid with counters and a "Run All Eligible" button.

---

### GAP-6: Delta table list not visible without triggering a download [Priority: MEDIUM]

**What's missing**: The only way to know which Delta tables were generated by a calibration is to click the download button in the History modal. The TablePicker only appears in a download context; there is no read-only "view tables" action.

**Impact**: Users cannot inspect what was produced without initiating a download flow.

**Proposed fix**: Add a "Tables" chip or section in CalibrationDetailModal (and history entries) that lists generated table names (catalog.schema.table) in a read-only way, with a separate download action per table.

---

### GAP-7: Error recovery path is unclear [Priority: MEDIUM]

**What's missing**: When a step fails, the error message is shown in CalibrationDetailModal (or CalibrationProgress step dots turn red), but there is no suggested action. The user does not know:
- Whether retrying will pick up from the failed step or restart from scratch
- Whether the "skip_sync" / "skip_copy" checkboxes can be used to skip the already-completed steps
- Whether the data from completed steps is still valid

**Impact**: Failed calibrations require users to navigate documentation or ask colleagues.

**Proposed fix**: When `job.status === "failed"`, show contextual guidance in the CountryCard and detail modal: "Step X failed. You can re-run with 'Skip data sync' enabled to resume from the copy step." Detect which step failed from `job.steps` and pre-fill the checkboxes accordingly.

---

### GAP-8: No period-level data staleness warning [Priority: LOW]

**What's missing**: The "Last calibration: Xd ago" badge on the card does not distinguish between calibration for the **selected period** vs any recent period. A card could show "Cal: 2h ago" but that was for last month, not the currently selected period.

**Impact**: Users may think a country is calibrated for the selected period when it is not.

**Proposed fix**: Change the "Cal" badge to reflect last calibration for the **selected period** specifically. The `useLastCalibration` hook should filter by period, or the badge should show "Cal: never (202502)" when the current period has not been run.

---

### GAP-9: No mobile-responsive detail view [Priority: LOW]

**What's missing**: CalibrationDetailModal uses `max-w-lg` and a grid layout that renders reasonably on desktop, but on small screens (375px) the step list becomes cramped and task keys overflow their containers.

**Impact**: Mobile users (or small laptop screens) have degraded readability in the detail modal.

**Proposed fix**: Stack the metadata grid to single column below `sm:` breakpoint, and truncate task keys with tooltip on hover.

---

### GAP-10: Config dialog has no reset/defaults button [Priority: LOW]

**What's missing**: Once `row_limit` or `lookback_months` are set, there is no one-click "Reset to defaults" button. The user must manually clear each field.

**Proposed fix**: Add a small "Reset" link below the overrides fieldset that sets `row_limit = null` and `lookback_months = null`.

---

## 3. Proposed UX Improvements

### 3.1 Excel Report Generation (addresses GAP-1)

**Where**: CalibrationDetailModal and CalibrationHistoryModal history entries.

**Flow**:
1. A "Generate Report" button appears next to "Download CSV" when `job.status === "completed"`.
2. Clicking it opens a small popover with report options:
   - Format: Excel (.xlsx) [default] / CSV
   - Include: Penetration, Volume, Both [radio group]
   - Aggregation level: matches what was used in the run (pre-filled, read-only)
3. "Generate" button fires a `POST /reports/generate` with `{job_id, format, kpi_types}`.
4. The button enters a "Generating..." state (spinner). Poll `GET /reports/{report_id}/status` every 3s.
5. On completion, the button changes to "Download Report" and triggers file download.
6. If generation fails, show an inline error with a "Retry" option.

**History modal**: Each completed row gets a split download button — left side = CSV (current behavior), right side = Excel (new).

---

### 3.2 KPI Summary / Validation Panel (addresses GAP-2)

**Where**: New tab within CalibrationDetailModal, or a new CalibrationResultsPage accessible from the history row.

**Layout (text wireframe)**:

```
+--------------------------------------------------+
| KPI Results — Chile  202502                       |
| [Overview] [By Product] [vs Previous]            |
+--------------------------------------------------+
| PENET    VOL1    TRANS    VAL_LC    HHOLDS        |
| 42.3%   1,204   2.8     $8,921   18,432          |
| [+1.2pp] [-0.8] [+0.1]  [+2.1%]  [+412]         |
|   vs 202501 (2 weeks ago)                        |
+--------------------------------------------------+
| Products out of tolerance (PENET delta > 5pp):   |
|  - Product 290: 38.1% -> 51.2% (+13.1pp) [!]    |
|  - Product 631: 22.0% -> 28.3% (+6.3pp) [!]     |
|                                                  |
| [View All Products]   [Download KPI Table]       |
+--------------------------------------------------+
```

Tolerance thresholds: configurable in CalibrationConfigDialog (e.g., "Alert if PENET delta > 5pp").

---

### 3.3 Summary Header Bar (addresses GAP-5)

**Where**: Above the country grid in CalibrationPage.

**Layout (text wireframe)**:

```
+---------------------------------------------------------------+
| Period: 202502                                                |
|   8 countries    5 calibrated    2 pending    1 never run     |
|   [Run All Eligible (3)]                                      |
+---------------------------------------------------------------+
[ Chile ]  [ Brazil ]  [ Colombia ]  ...
```

"Run All Eligible" sends calibration requests for all countries where:
- `availability.elegibilidad === true`
- No job is currently running
- The selected period has not been calibrated yet (or was last calibrated > N days ago)

Shows a progress count: "3/3 started" as each job is triggered sequentially.

---

### 3.4 Contextual Error Recovery (addresses GAP-7)

**Where**: CountryCard (below progress bar when failed) and CalibrationDetailModal header.

**Layout (text wireframe)**:

```
+----------------------------------+
| [!] Calibration failed           |
|     Step "simulate_kpis" failed  |
|     Error: OOM on driver node    |
|                                  |
| [Re-run (skip sync + copy)]      |
| [View full error]                |
+----------------------------------+
```

Logic:
- If `sync_data` completed → offer "Skip sync"
- If `copy_to_calibration` completed → offer "Skip copy"
- Pre-fill checkboxes in the confirm dialog based on which steps succeeded

---

### 3.5 Per-Country Config Override (addresses GAP-4)

**Where**: AlertDialog (confirm calibration) — expand the existing dialog.

**Layout addition (text wireframe)**:

```
Confirm Calibration — Ecuador  202502

Aggregations: [none]
Overrides: [none]

[v] Country-specific overrides
    Lookback months: [13 ___]
    Row limit: [______]
    Group by: [1D - Product only v]

[Cancel]  [Confirm]
```

The "Country-specific overrides" section is collapsed by default and shows the global config values as placeholders.

---

## 4. Wireframe Descriptions for New Components

### 4.1 CalibrationResultsPanel (new, inside detail modal)

A tabbed panel added to CalibrationDetailModal after the existing pipeline steps section.

**Tab 1 — Overview**:
- A 5-column stat row (PENET, VOL1, TRANS, VAL_LC, HHOLDS) with large numbers and delta vs previous run in smaller text below (colored green/red)
- A "Comparison period" selector (dropdown: "vs previous period", "vs same period last year")
- A "Tolerance alerts" section listing products/rows where any KPI delta exceeds configured thresholds

**Tab 2 — By Product**:
- A sortable data table (product code, PENET, VOL1, TRANS, delta columns)
- Search/filter input above the table
- "Export to Excel" button in top-right corner of the tab

**Tab 3 — vs Previous Run**:
- Side-by-side comparison of current vs previous run metadata (date, duration, rows extracted, period)
- KPI delta summary table

---

### 4.2 Report Generation Popover

A `Popover` component attached to a "Generate Report" button in CalibrationDetailModal.

**Contents**:
- Section heading: "Export Report"
- Radio group: "KPI Type" — Penetration / Volume / Both
- Radio group: "Format" — Excel (.xlsx) / CSV
- Read-only display: "Aggregation: 1D" (from job params)
- Primary button: "Generate" (disabled until KPI Type selected)
- After generation starts: spinner + "Generating report..." text + optional cancel link

---

### 4.3 CalibrationSummaryBar (new, at top of CalibrationPage)

A horizontal band between the page header and the country grid.

**Contents (left to right)**:
- "8 countries" counter badge
- "5 calibrated for 202502" badge (green)
- "2 not yet calibrated" badge (amber)
- "1 running" badge with spinner (blue, only shown if any job running)
- Spacer
- "Run All Eligible" button (right-aligned, only enabled when ≥1 country is eligible and not running)

---

### 4.4 History Detail Button (addition to CalibrationHistoryModal)

Each row in the history list gains a second action button (eye icon) to the left of the download button:

```
[ Calendar icon ] Mar 1, 2026  14:32
  Period: 202502
  1,293,412 rows extracted

                    [Completed]  8m 42s  [ Eye ] [ Download ]
```

Clicking the eye icon opens CalibrationDetailModal loaded with that historical `job_id`'s `EventDetail`. This reuses the existing modal with a `useQuery` fetch on demand.

---

## 5. Priority Summary

| Gap | Description | Priority | Effort |
|-----|-------------|----------|--------|
| GAP-1 | Excel report generation | HIGH | Large |
| GAP-2 | KPI validation/comparison panel | HIGH | Large |
| GAP-3 | Eye icon only in session | HIGH | Small |
| GAP-4 | Per-country config overrides | MEDIUM | Medium |
| GAP-5 | Aggregate status + Run All | MEDIUM | Medium |
| GAP-6 | Read-only table listing | MEDIUM | Small |
| GAP-7 | Error recovery guidance | MEDIUM | Small |
| GAP-8 | Period-specific staleness badge | LOW | Small |
| GAP-9 | Mobile responsiveness | LOW | Small |
| GAP-10 | Config reset button | LOW | Trivial |

**Quick wins (small effort, high value)**: GAP-3, GAP-6, GAP-7, GAP-10.
**Core workflow completers**: GAP-1, GAP-2.
**Power user features**: GAP-4, GAP-5.
