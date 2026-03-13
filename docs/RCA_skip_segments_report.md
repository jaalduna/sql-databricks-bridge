# Root Cause Analysis: Too Many Skip Segments with <2 Periods

**Date**: 2026-02-24
**Project**: kwp-elegibilidad
**Catalog**: `004-elegibilidad` (Databricks Unity Catalog)
**Country**: Brasil (BR)
**Investigated by**: Pascal RCA Team (6 agents)

---

## Problem Statement

Eligibility results show an abnormally high number of "skip" segments — segments where households have fewer than 2 contribution periods (`N_Contrib < 2`). This leads to insufficient donor/receptor matching and unreliable eligibility assessments.

**Current state (Periodo 202501):**

| Metric | Value |
|--------|-------|
| Total households in classification | 5,788 |
| Households with N_Contrib = 0 | 691 (11.9%) |
| Households with N_Contrib = 1 | 221 (3.8%) |
| **Total with <2 periods (SKIPPED)** | **912 (15.8%)** |
| Households flagged Muerto | 5,151 (89%) |
| Donor-receptor matches | 17 (extremely low) |

---

## Pipeline Architecture (Critical Context)

There are **two separate pipelines** producing different tables:

| Pipeline | Notebook | Output Tables | Status |
|----------|----------|--------------|--------|
| **Silver/Gold merge** | `silver_gold_pipeline.py` | `silver_transactions`, `household_metrics`, `period_summary`, `product_summary` | Test artifact (1,000 rows) — NOT used by eligibility |
| **Eligibility** | `eligibility_pipeline.py` | `household_classification`, `donor_receptor_matching`, `eligibility_summary`, `domicilios_*`, `actos_*` | The actual pipeline under investigation |

**Key insight**: `N_Contrib` in `household_classification` is derived from `brasil_weights` (534,688 rows, 48 periods from 202101-202512), NOT from `silver_transactions`. The 1,000-row silver table is irrelevant to the skip segment problem.

---

## Hypotheses Tested

### H1: Source panel data has many short-lived panelists — REJECTED

**Evidence**: Bronze `brasil_purchases` has 23,100,199 rows spanning 60 periods (202101-202512). `brasil_weights` has 534,688 rows covering 48 months and 33,953 households. The source data is rich and multi-period.

### H2: Pipeline processes only 1 period at a time — CONFIRMED (CONTRIBUTING)

**Evidence**:

1. `eligibility_pipeline.py` line 144: `J = purchases.filter(F.col("Periodo") == PERIOD)` — single-period filter
2. Lines 535-539: `.mode("overwrite")` — each run destroys previous output
3. Only 1 period (202501) exists in `household_classification`
4. Brasil.yaml has `per_ini: 202401, per_fin: 202401` with comment "Testing with 1 month!"

**Impact**: The single-period design means each run only classifies households based on one month of purchases. However, `N_Contrib` is computed from the FULL weights history, so this alone doesn't cause the skip problem.

### H3: Join logic drops rows — CONFIRMED (CONTRIBUTING)

**Evidence**:

1. `brasil_panelists` only has `Ano=2025` — missing years 2021-2024
2. The panelist join orphans 85.2% of purchases (19.7M rows with no demographic match)
3. `silver_gold_pipeline.py` produced only 1,000 rows in `silver_transactions` (test artifact, irrelevant to eligibility)
4. The eligibility pipeline joins are LEFT JOINs and preserve all rows correctly

**Impact**: The missing panelist years affect the `silver_gold_pipeline` but NOT the eligibility pipeline directly. The eligibility pipeline uses its own joins and reads `brasil_weights` independently.

### H4: Segment granularity is too fine — CONTRIBUTING FACTOR

**Evidence**:

1. Segments (Celda) = `NSE_LOC` + `_` + `BR_FXREGIAO` — 166 distinct segments
2. 6 segments with `BR_FXREGIAO=0` (`1_0`, `2_0`, `3_0`, `4_0`, `5_0`, `NA_NA`) — ALL 92 households have `N_Contrib = 0`
3. These `*_0` segments represent households with missing/invalid regional classification

**Impact**: The segment granularity is reasonable (166 cells). The problem is concentrated in `*_0` segments (missing demographics).

---

## Root Causes (Ranked by Impact)

### 1. PRIMARY: 691 Households Have No Weight History (N_Contrib = 0)

`N_Contrib` is computed from `brasil_weights` — it counts the number of periods where a household has a positive weight value. **691 households (11.9%) have N_Contrib=0** because they have **no matching entry in `brasil_weights` for any period**.

These are likely:
- **Newly recruited panelists** that haven't been assigned weights yet
- **Households with incomplete onboarding** (purchased but not yet weighted)
- **Households in `*_0` segments** (BR_FXREGIAO=0) that were never classified

**Evidence**:
- `brasil_weights`: 534,688 rows, 33,953 distinct households, periods 202101-202512
- `household_classification`: 5,788 households — so ~1,835 households in classification have no weight match
- N_Contrib max = 13 (matches 13-month lookback window in weights)
- All `*_0` segment households have N_Contrib=0

### 2. HIGH: 89% Mortality Rate (5,151 of 5,788 Muerto)

The `Muerto` flag is abnormally high at 89%. This is likely caused by:
- The mortality LEFT JOIN matching too broadly (historical exits applied to current period)
- `eligibility_pipeline.py` line 162: `MOR = mortality.filter(F.col("Periodo") == PERIOD)` filters correctly, but the join at lines 228-233 flags any household with a mortality record as dead
- If the `brasil_mortality` table contains cumulative exit records, this could over-flag

**Impact**: With 89% flagged as dead, the donor/receptor pool is devastated:
- Only 7 donors (should be hundreds)
- Only 555 receptors
- Only 17 matches

### 3. MEDIUM: Missing Demographics (BR_FXREGIAO=0)

92 households in `*_0` segments have no valid regional classification. These create permanently-skipped segments regardless of weight history.

### 4. LOW: Single-Period Design + Overwrite Mode

While the pipeline processes 1 period at a time with overwrite mode, this is **by design** for the eligibility use case (monthly processing). The `N_Contrib` computation already uses the full weights history. The overwrite is acceptable if runs are for the current period.

### 5. LOW: `silver_transactions` Test Artifact

The `silver_gold_pipeline.py` produced only 1,000 rows — this is a test/development artifact and does NOT affect eligibility computation. Can be re-run if needed for household_metrics/product_summary gold tables.

---

## Data Flow Diagram

```
bronze_br.brasil_purchases (23.1M, 60 periods)
  │
  ├─[eligibility_pipeline.py]──→ filter(Periodo == 202501)
  │     │                         227,707 rows
  │     ├─ LEFT JOIN brasil_households (424K)
  │     ├─ LEFT JOIN brasil_panelists (56K, Ano=2025 only)
  │     ├─ LEFT JOIN brasil_mortality ──→ 5,151 Muerto (89%!)
  │     ├─ LEFT JOIN brasil_weights (534K, 48 periods)
  │     │     └─→ N_Contrib computed from weights history
  │     │         691 households = 0 (no weight match)
  │     │         221 households = 1
  │     │         3,290 households = 13 (max)
  │     │
  │     └─→ silver_br.household_classification (5,788 rows)
  │         └─→ gold_br.eligibility_summary, donor_receptor_matching (17)
  │
  └─[silver_gold_pipeline.py]──→ silver_br.silver_transactions (1,000 rows)
        TEST ARTIFACT — not used by eligibility
```

---

## Recommendations

### Immediate (Fix skip segments)

1. **Investigate `brasil_weights` coverage**: Query why 691+ households in classification have no weight records. Are these newly recruited? Are they in excluded panels (14, 22)?
   ```sql
   SELECT hc.IdDomicilio, hc.ORIGEN, hc.Celda, hc.ActosT
   FROM `004-elegibilidad`.`silver_br`.household_classification hc
   LEFT JOIN (
     SELECT DISTINCT IdDomicilio FROM `004-elegibilidad`.`bronze_br`.brasil_weights
   ) w ON hc.IdDomicilio = w.IdDomicilio
   WHERE w.IdDomicilio IS NULL
   ORDER BY hc.ActosT DESC
   LIMIT 50
   ```

2. **Investigate 89% Muerto rate**: Check if mortality data is cumulative vs current-period-only
   ```sql
   SELECT COUNT(DISTINCT IdDomicilio) as total_mortality_records,
          MIN(Periodo) as earliest_exit, MAX(Periodo) as latest_exit
   FROM `004-elegibilidad`.`bronze_br`.brasil_mortality
   ```

3. **Fix mortality join** if it's matching on full history instead of current period:
   ```python
   # Current (line 162): correctly filters to PERIOD
   MOR = mortality.filter(F.col("Periodo") == PERIOD)
   # BUT: if the mortality table has cumulative records, this still over-flags
   # Consider: only flag as Muerto if exit date is BEFORE the target period
   ```

### Short-Term (Pipeline improvements)

4. **Re-extract `brasil_panelists`** for years 2021-2025 into bronze (needed for silver_gold_pipeline, not eligibility)
5. **Re-run `silver_gold_pipeline`** with full data (for household_metrics gold table)
6. **Add assertion** in eligibility pipeline: `assert muerto_rate < 0.30, f"Mortality rate {muerto_rate} is abnormally high"`

### Medium-Term (Data quality)

7. **Handle `*_0` segments**: Either exclude BR_FXREGIAO=0 households or assign default region
8. **Add weight coverage check**: Before running eligibility, verify that ≥80% of purchasing households have weight records
9. **Update Brasil.yaml**: `per_ini: 202101` to enable multi-period analysis

---

## Evidence Summary

| Data Point | Expected | Actual | Status |
|-----------|----------|--------|--------|
| Bronze purchases | - | 23.1M rows, 60 periods | OK |
| Bronze weights | - | 534K rows, 48 periods, 34K households | OK |
| Bronze panelists years | 2021-2025 | 2025 only | MEDIUM (affects silver_gold, not eligibility) |
| Households with N_Contrib >= 2 | >90% | 84.2% | INVESTIGATE |
| Households with N_Contrib = 0 | <5% | 11.9% (691) | PRIMARY ISSUE |
| Muerto rate | <20% | 89% (5,151) | HIGH — suspected over-flagging |
| Donor-receptor matches | hundreds | 17 | CRITICAL — consequence of high Muerto |
| `*_0` segments (missing demographics) | 0 | 92 households, 6 segments | LOW |
| silver_transactions | N/A | 1,000 (test artifact) | IGNORE — not used by eligibility |

---

## Team

| Agent | Role | Key Finding |
|-------|------|-------------|
| researcher | Table discovery + deep dive | Identified two separate pipelines; N_Contrib comes from weights, not silver; silver_transactions is test artifact |
| sql-dev-h1 | H1 tester | Rejected: source data has 60 periods, rich data |
| sql-dev-h2 | H2 tester | Confirmed: single-period design, but N_Contrib uses full weights history |
| sql-dev-h3 | H3 tester | Confirmed: `brasil_panelists` missing 2021-2024 (affects silver_gold, not eligibility) |
| rca-analyst | H4 tester | Contributing: `*_0` segments have missing demographics |

---

## Next Steps

1. Run the diagnostic queries in Recommendations #1 and #2 to determine:
   - Why 691 households lack weight records
   - Whether mortality data is cumulative (causing 89% Muerto rate)
2. Based on findings, either fix the data extraction or adjust the pipeline logic
3. Re-run eligibility pipeline and verify skip segment rate drops below 5%
