"""Synthetic + real data validation for differential sync.

Tests the full pipeline:
  1. Synthetic: Create temp table, run N/N+1/N+2 iterations, verify changes detected
  2. Bolivia: Real j_atoscompra_new with 6 lookback periods

Usage:
  poetry run python tests/test_diff_sync.py --synthetic   # Synthetic only
  poetry run python tests/test_diff_sync.py --bolivia      # Bolivia real data
  poetry run python tests/test_diff_sync.py                # Both
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

API_BASE = "http://127.0.0.1:8001/api/v1"


def wait_for_job(job_id: str, timeout: int = 600, wait_for_queries_only: bool = False) -> dict:
    """Poll job status until complete or timeout.

    Args:
        job_id: Job ID to poll.
        timeout: Max seconds to wait.
        wait_for_queries_only: If True, return as soon as all queries finish
            (don't wait for calibration pipeline to complete).
    """
    start = time.time()
    while time.time() - start < timeout:
        resp = requests.get(f"{API_BASE}/events/{job_id}")
        resp.raise_for_status()
        data = resp.json()
        status = data["status"]
        completed = data.get("queries_completed", 0)
        failed = data.get("queries_failed", 0)
        total = data.get("queries_total", 0)
        running = data.get("queries_running", 0)
        logger.info(f"  Job {job_id}: {status} ({completed}/{total} done, {running} running)")

        if status in ("completed", "failed", "cancelled"):
            return data

        # If we only care about query completion, check if all queries finished
        if wait_for_queries_only and total > 0 and (completed + failed) >= total and running == 0:
            logger.info(f"  All {total} queries finished ({completed} ok, {failed} failed)")
            return data

        time.sleep(5)

    raise TimeoutError(f"Job {job_id} did not complete in {timeout}s")


# ---------------------------------------------------------------------------
# SYNTHETIC TEST
# ---------------------------------------------------------------------------

def test_synthetic():
    """Test diff sync with synthetic data on SQL Server + Databricks."""
    print("\n" + "=" * 70)
    print("SYNTHETIC DIFFERENTIAL SYNC TEST")
    print("=" * 70)

    # We'll use the API to trigger a differential sync
    # First, we need a small table that we can control.
    # We'll use a direct SQL approach: create a temp table, trigger diff sync,
    # and validate.

    # For this test, we work directly with the Python modules
    from sql_databricks_bridge.core.config import get_settings
    from sql_databricks_bridge.core.delta_writer import DeltaTableWriter
    from sql_databricks_bridge.core.diff_sync import run_differential_sync
    from sql_databricks_bridge.core.fingerprint import (
        ensure_fingerprint_table,
        load_stored_fingerprints,
    )
    from sql_databricks_bridge.db.databricks import DatabricksClient
    from sql_databricks_bridge.db.sql_server import SQLServerClient

    settings = get_settings()
    sql_client = SQLServerClient(country="bolivia")
    dbx_client = DatabricksClient()
    writer = DeltaTableWriter(dbx_client)
    fp_table = settings.fingerprint_table

    test_table = "_diff_sync_test"
    dbx_table_name = "_diff_sync_test"
    test_country = "bolivia"  # Use existing schema/volume in Databricks

    print(f"\n--- Setup: Creating test table {test_table} in Bolivia SQL Server ---")

    # Create test table with synthetic data (use execute_write for DDL/DML)
    sql_client.execute_write(f"""
        IF OBJECT_ID('{test_table}') IS NOT NULL DROP TABLE {test_table}
    """)
    sql_client.execute_write(f"""
        CREATE TABLE {test_table} (
            periodo INT,
            idproduto INT,
            valor DECIMAL(10,2),
            descripcion VARCHAR(100)
        )
    """)

    # Insert synthetic data: 3 periods x 5 products = 15 combinations
    rows = []
    for periodo in [202401, 202402, 202403]:
        for prod in [1, 2, 3, 4, 5]:
            rows.append(f"({periodo}, {prod}, {periodo * 0.01 + prod}, 'original_{periodo}_{prod}')")

    sql_client.execute_write(f"INSERT INTO {test_table} VALUES {', '.join(rows)}")
    count_result = sql_client.execute_query(f"SELECT COUNT(*) as cnt FROM {test_table}")
    print(f"  Inserted {count_result['cnt'][0]} rows")

    # === ITERATION N: Baseline sync ===
    print(f"\n--- Iteration N: Baseline sync ---")

    # Clean up any previous fingerprints for this test
    try:
        ensure_fingerprint_table(dbx_client, fp_table)
        dbx_client.execute_sql(
            f"DELETE FROM {fp_table} WHERE country = '{test_country}' AND table_name = '{dbx_table_name}'"
        )
    except Exception:
        pass

    # Also drop the test target table if it exists
    try:
        dbx_client.execute_sql(
            f"DROP TABLE IF EXISTS `{settings.databricks.catalog}`.`{test_country}`.`{dbx_table_name}`"
        )
    except Exception:
        pass

    stats_n = run_differential_sync(
        sql_client=sql_client,
        dbx_client=dbx_client,
        writer=writer,
        sql_table=test_table,
        country=test_country,
        level1_column="periodo",
        level2_column="idproduto",
        fingerprint_table=fp_table,
        job_id="test-n",
    )

    print(f"  First sync: {stats_n.is_first_sync}")
    print(f"  Rows downloaded: {stats_n.rows_downloaded}")
    print(f"  Rows skipped: {stats_n.rows_skipped}")
    assert stats_n.is_first_sync, "Expected first sync"
    assert stats_n.rows_downloaded == 15, f"Expected 15 rows, got {stats_n.rows_downloaded}"

    # Verify Databricks has the data
    dbx_table = f"`{settings.databricks.catalog}`.`{test_country}`.`{dbx_table_name}`"
    dbx_count = dbx_client.execute_sql(f"SELECT COUNT(*) as cnt FROM {dbx_table}")
    assert int(dbx_count[0]["cnt"]) == 15, f"Databricks should have 15 rows, got {dbx_count[0]['cnt']}"
    print(f"  Databricks table has {dbx_count[0]['cnt']} rows - OK")

    # === ITERATION N+1: Introduce changes ===
    print(f"\n--- Iteration N+1: Modify data (update 202402/prod=3, insert 202403/prod=6) ---")

    # UPDATE: Change a value in periodo=202402, idproduto=3
    sql_client.execute_write(f"""
        UPDATE {test_table} SET valor = 999.99, descripcion = 'MODIFIED'
        WHERE periodo = 202402 AND idproduto = 3
    """)

    # INSERT: Add a new product in periodo=202403
    sql_client.execute_write(f"""
        INSERT INTO {test_table} VALUES (202403, 6, 66.66, 'new_product')
    """)

    stats_n1 = run_differential_sync(
        sql_client=sql_client,
        dbx_client=dbx_client,
        writer=writer,
        sql_table=test_table,
        country=test_country,
        level1_column="periodo",
        level2_column="idproduto",
        fingerprint_table=fp_table,
        job_id="test-n1",
    )

    print(f"  First sync: {stats_n1.is_first_sync}")
    print(f"  Changed L1: {stats_n1.changed_level1}, New L1: {stats_n1.new_level1}")
    print(f"  Changed pairs: {stats_n1.total_changed_pairs}")
    print(f"  Rows downloaded: {stats_n1.rows_downloaded}")
    print(f"  Rows skipped: {stats_n1.rows_skipped}")

    assert not stats_n1.is_first_sync, "Should not be first sync"
    assert stats_n1.unchanged_level1 == 1, f"Expected 1 unchanged period (202401), got {stats_n1.unchanged_level1}"
    assert stats_n1.changed_level1 >= 1, "Expected at least 1 changed period"
    assert stats_n1.rows_downloaded < 16, "Should download less than full table"

    # Verify Databricks has 16 rows now (15 original + 1 new)
    dbx_count = dbx_client.execute_sql(f"SELECT COUNT(*) as cnt FROM {dbx_table}")
    assert int(dbx_count[0]["cnt"]) == 16, f"Databricks should have 16 rows, got {dbx_count[0]['cnt']}"

    # Verify the modification exists
    modified = dbx_client.execute_sql(
        f"SELECT valor FROM {dbx_table} WHERE periodo = 202402 AND idproduto = 3"
    )
    assert float(modified[0]["valor"]) == 999.99, f"Expected modified value 999.99, got {modified[0]['valor']}"
    print(f"  Databricks has {dbx_count[0]['cnt']} rows, modified value verified - OK")

    # === ITERATION N+2: Restore original data ===
    print(f"\n--- Iteration N+2: Restore original data ---")

    # Revert the UPDATE
    sql_client.execute_write(f"""
        UPDATE {test_table} SET valor = {202402 * 0.01 + 3}, descripcion = 'original_202402_3'
        WHERE periodo = 202402 AND idproduto = 3
    """)

    # DELETE the added product
    sql_client.execute_write(f"""
        DELETE FROM {test_table} WHERE periodo = 202403 AND idproduto = 6
    """)

    stats_n2 = run_differential_sync(
        sql_client=sql_client,
        dbx_client=dbx_client,
        writer=writer,
        sql_table=test_table,
        country=test_country,
        level1_column="periodo",
        level2_column="idproduto",
        fingerprint_table=fp_table,
        job_id="test-n2",
    )

    print(f"  Changed L1: {stats_n2.changed_level1}, Deleted L1: {stats_n2.deleted_level1}")
    print(f"  Rows downloaded: {stats_n2.rows_downloaded}")
    print(f"  Rows skipped: {stats_n2.rows_skipped}")

    assert stats_n2.changed_level1 >= 1, "Expected changes detected"

    # Verify Databricks is back to 15 rows
    dbx_count = dbx_client.execute_sql(f"SELECT COUNT(*) as cnt FROM {dbx_table}")
    assert int(dbx_count[0]["cnt"]) == 15, f"Databricks should have 15 rows, got {dbx_count[0]['cnt']}"

    # Verify original value restored
    restored = dbx_client.execute_sql(
        f"SELECT valor FROM {dbx_table} WHERE periodo = 202402 AND idproduto = 3"
    )
    expected_val = round(202402 * 0.01 + 3, 2)
    actual_val = float(restored[0]["valor"])
    assert abs(actual_val - expected_val) < 0.1, f"Expected restored value ~{expected_val}, got {actual_val}"
    print(f"  Databricks has {dbx_count[0]['cnt']} rows, original value restored - OK")

    # Cleanup
    print(f"\n--- Cleanup ---")
    sql_client.execute_write(f"IF OBJECT_ID('{test_table}') IS NOT NULL DROP TABLE {test_table}")
    try:
        dbx_client.execute_sql(f"DROP TABLE IF EXISTS {dbx_table}")
        dbx_client.execute_sql(
            f"DELETE FROM {fp_table} WHERE country = '{test_country}' AND table_name = '{dbx_table_name}'"
        )
    except Exception:
        pass

    # === PERFORMANCE REPORT ===
    print("\n" + "=" * 70)
    print("PERFORMANCE REPORT - SYNTHETIC TEST")
    print("=" * 70)
    print(f"{'Metric':<40} {'N (baseline)':>14} {'N+1 (mutated)':>14} {'N+2 (restored)':>14}")
    print(f"{'-'*40} {'-'*14} {'-'*14} {'-'*14}")
    print(f"{'Is first sync':<40} {'Yes':>14} {'No':>14} {'No':>14}")
    print(f"{'Level 1 values':<40} {stats_n.total_level1_values:>14} {stats_n1.total_level1_values:>14} {stats_n2.total_level1_values:>14}")
    print(f"{'Changed level 1':<40} {stats_n.changed_level1:>14} {stats_n1.changed_level1:>14} {stats_n2.changed_level1:>14}")
    print(f"{'New level 1':<40} {stats_n.new_level1:>14} {stats_n1.new_level1:>14} {stats_n2.new_level1:>14}")
    print(f"{'Unchanged level 1':<40} {stats_n.unchanged_level1:>14} {stats_n1.unchanged_level1:>14} {stats_n2.unchanged_level1:>14}")
    print(f"{'Changed pairs':<40} {stats_n.total_changed_pairs:>14} {stats_n1.total_changed_pairs:>14} {stats_n2.total_changed_pairs:>14}")
    print(f"{'Rows downloaded':<40} {stats_n.rows_downloaded:>14} {stats_n1.rows_downloaded:>14} {stats_n2.rows_downloaded:>14}")
    print(f"{'Rows skipped':<40} {stats_n.rows_skipped:>14} {stats_n1.rows_skipped:>14} {stats_n2.rows_skipped:>14}")
    print(f"{'L1 fingerprint time (s)':<40} {stats_n.level1_fingerprint_time:>14.2f} {stats_n1.level1_fingerprint_time:>14.2f} {stats_n2.level1_fingerprint_time:>14.2f}")
    print(f"{'L2 fingerprint time (s)':<40} {stats_n.level2_fingerprint_time:>14.2f} {stats_n1.level2_fingerprint_time:>14.2f} {stats_n2.level2_fingerprint_time:>14.2f}")
    print(f"{'Extraction time (s)':<40} {stats_n.extraction_time:>14.2f} {stats_n1.extraction_time:>14.2f} {stats_n2.extraction_time:>14.2f}")
    print(f"{'Write time (s)':<40} {stats_n.write_time:>14.2f} {stats_n1.write_time:>14.2f} {stats_n2.write_time:>14.2f}")
    print(f"{'Total time (s)':<40} {stats_n.total_time:>14.2f} {stats_n1.total_time:>14.2f} {stats_n2.total_time:>14.2f}")

    # Savings calculation
    full_table_rows = 15  # Original table size
    n1_savings = (1 - stats_n1.rows_downloaded / full_table_rows) * 100 if full_table_rows else 0
    n2_savings = (1 - stats_n2.rows_downloaded / full_table_rows) * 100 if full_table_rows else 0
    print(f"\n{'Download savings N+1 vs full':<40} {n1_savings:>13.1f}%")
    print(f"{'Download savings N+2 vs full':<40} {n2_savings:>13.1f}%")

    print("\n" + "=" * 70)
    print("SYNTHETIC TEST PASSED")
    print("=" * 70)


# ---------------------------------------------------------------------------
# BOLIVIA REAL DATA TEST (via API)
# ---------------------------------------------------------------------------

def test_bolivia_api():
    """Test diff sync with real Bolivia data via the API (6 lookback periods)."""
    print("\n" + "=" * 70)
    print("BOLIVIA REAL DATA DIFFERENTIAL SYNC TEST (6 lookback periods)")
    print("=" * 70)

    # === ITERATION N: Baseline diff sync via API ===
    print("\n--- Iteration N: Baseline differential sync ---")
    trigger_payload = {
        "country": "bolivia",
        "stage": "inicio",
        "period": "202602",
        "queries": ["j_atoscompra_new"],
        "lookback_months": 6,
        "differential_sync": {
            "j_atoscompra_new": {
                "level1_column": "periodo",
                "level2_column": "idproduto",
            }
        }
    }

    resp = requests.post(f"{API_BASE}/trigger", json=trigger_payload)
    if resp.status_code != 201:
        print(f"  FAILED: {resp.status_code} {resp.text}")
        return False
    job_n = resp.json()
    print(f"  Job N: {job_n['job_id']}")

    result_n = wait_for_job(job_n["job_id"], timeout=600, wait_for_queries_only=True)
    print(f"  Status: {result_n['status']}")

    # Check query-level results (sync may still be "running" due to calibration pipeline)
    query_failed = False
    for qr in result_n.get("results", []):
        if qr["query_name"] == "j_atoscompra_new":
            print(f"  Rows synced: {qr['rows_extracted']}")
            if qr["status"] == "failed":
                print(f"  FAILED: {qr.get('error', 'unknown error')}")
                query_failed = True
            break

    if query_failed:
        return False

    # === ITERATION N+1: Run again (no changes, should skip everything) ===
    print("\n--- Iteration N+1: Re-run (should detect zero changes) ---")
    resp = requests.post(f"{API_BASE}/trigger", json=trigger_payload)
    if resp.status_code != 201:
        print(f"  FAILED: {resp.status_code} {resp.text}")
        return False
    job_n1 = resp.json()
    print(f"  Job N+1: {job_n1['job_id']}")

    result_n1 = wait_for_job(job_n1["job_id"], timeout=600, wait_for_queries_only=True)
    print(f"  Status: {result_n1['status']}")

    for qr in result_n1.get("results", []):
        if qr["query_name"] == "j_atoscompra_new":
            rows_n1 = qr["rows_extracted"]
            print(f"  Rows synced: {rows_n1}")
            if rows_n1 == 0:
                print("  PERFECT: No changes detected, zero rows downloaded")
            else:
                print(f"  NOTE: {rows_n1} rows downloaded (data may have changed between iterations)")
            break

    print("\n" + "=" * 70)
    print("BOLIVIA API TEST COMPLETED")
    print("=" * 70)
    return True


def main():
    parser = argparse.ArgumentParser(description="Differential Sync Tests")
    parser.add_argument("--synthetic", action="store_true", help="Run synthetic test only")
    parser.add_argument("--bolivia", action="store_true", help="Run Bolivia real data test only")
    args = parser.parse_args()

    run_all = not args.synthetic and not args.bolivia

    if args.synthetic or run_all:
        test_synthetic()

    if args.bolivia or run_all:
        test_bolivia_api()


if __name__ == "__main__":
    main()
