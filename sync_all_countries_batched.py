"""Trigger j_atoscompra_new diff sync for all 9 countries in batches of 3.

Runs countries in batches to avoid overwhelming SQL Server with 9 concurrent
long-running queries. Each batch of 3 completes before the next starts.
"""
import json
import time
import requests
from datetime import datetime

API_BASE = "http://127.0.0.1:8001/api/v1"
STAGE = "inicio"
PERIOD = "202602"
LOOKBACK = 60
QUERY = "j_atoscompra_new"

# Batch countries to avoid overwhelming SQL Server.
# Bolivia/Argentina/Ecuador already have partial fingerprints from prior run.
# Chile/CAM/Colombia/Mexico/Brasil/Peru will be first-sync (bulk extraction).
BATCHES = [
    ["bolivia", "ecuador", "peru"],       # Batch 1: 1 incremental + 2 first-sync
    ["argentina", "chile", "cam"],         # Batch 2: 1 incremental + 2 first-sync
    ["mexico", "brasil", "colombia"],      # Batch 3: all first-sync
]

POLL_INTERVAL = 30  # seconds between status checks


def trigger(country):
    payload = {
        "country": country,
        "stage": STAGE,
        "period": PERIOD,
        "queries": [QUERY],
        "lookback_months": LOOKBACK,
        "differential_sync": {
            QUERY: {
                "level1_column": "periodo",
                "level2_column": "idproduto",
            }
        },
    }
    try:
        resp = requests.post(f"{API_BASE}/trigger", json=payload, timeout=30)
        if resp.status_code == 201:
            data = resp.json()
            return data["job_id"]
        else:
            print(f"  FAILED {country}: {resp.status_code} {resp.text[:200]}")
            return None
    except Exception as e:
        print(f"  ERROR triggering {country}: {e}")
        return None


def check_job(job_id):
    try:
        resp = requests.get(f"{API_BASE}/events/{job_id}", timeout=10)
        if resp.status_code != 200:
            return None
        return resp.json()
    except:
        return None


def is_job_done(data):
    """Check if a job is finished (success or failure)."""
    if not data:
        return False
    status = data.get("status", "")
    if status in ("completed", "failed", "cancelled"):
        return True
    # Also check if all queries are done
    q_total = data.get("queries_total", 0)
    q_done = data.get("queries_completed", 0)
    q_fail = data.get("queries_failed", 0)
    q_run = data.get("queries_running", 0)
    if q_total > 0 and (q_done + q_fail) >= q_total and q_run == 0:
        return True
    return False


def run_batch(batch_num, countries, all_results):
    """Trigger and monitor a batch of countries until all complete."""
    print(f"\n{'='*70}")
    print(f"  BATCH {batch_num}: {', '.join(countries)}")
    print(f"  Started at {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*70}\n")

    jobs = {}
    for country in countries:
        job_id = trigger(country)
        if job_id:
            jobs[country] = {"job_id": job_id, "start": time.time()}
            print(f"  {country}: triggered (job {job_id[:8]}...)")
        time.sleep(1)

    if not jobs:
        print("  No jobs triggered in this batch!")
        return

    # Poll until all done
    completed = set()
    while len(completed) < len(jobs):
        time.sleep(POLL_INTERVAL)
        now = datetime.now().strftime("%H:%M:%S")

        for country, info in jobs.items():
            if country in completed:
                continue
            data = check_job(info["job_id"])
            if not data:
                continue

            status = data["status"]
            q_total = data.get("queries_total", 0)
            q_done = data.get("queries_completed", 0)
            q_fail = data.get("queries_failed", 0)
            q_run = data.get("queries_running", 0)
            rows = data.get("total_rows_extracted", 0)
            current_step = data.get("current_step", "")
            elapsed = time.time() - info["start"]

            if is_job_done(data):
                completed.add(country)
                info["rows"] = rows
                info["duration"] = elapsed
                info["status"] = status
                info["step"] = current_step
                info["q_fail"] = q_fail
                all_results[country] = info
                marker = "OK" if q_fail == 0 and rows > 0 else "WARN"
                print(f"  [{now}] {country:12s}: DONE [{marker}]  {rows:>12,} rows  {elapsed/60:>6.1f}min  step={current_step}")
            else:
                print(f"  [{now}] {country:12s}: {status:8s} {q_done}/{q_total}  {rows:>10,} rows  {elapsed/60:>5.1f}min  step={current_step}")

    print(f"\n  Batch {batch_num} complete at {datetime.now().strftime('%H:%M:%S')}")


def main():
    total_start = time.time()
    all_results = {}

    print(f"{'='*70}")
    print(f"  9-COUNTRY DIFFERENTIAL SYNC - BATCHED EXECUTION")
    print(f"  Query: {QUERY}, Lookback: {LOOKBACK} months, Diff sync: ON")
    print(f"  Batches: {len(BATCHES)} x {len(BATCHES[0])} countries")
    print(f"  Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")

    for i, batch in enumerate(BATCHES, 1):
        run_batch(i, batch, all_results)
        if i < len(BATCHES):
            print(f"\n  Waiting 10s before next batch...")
            time.sleep(10)

    # Summary
    total_elapsed = time.time() - total_start
    print(f"\n{'='*70}")
    print(f"  FINAL SUMMARY")
    print(f"{'='*70}\n")
    print(f"{'Country':12s} {'Rows':>12s} {'Duration':>10s} {'Status':>10s} {'Q.Fail':>8s}")
    print("-" * 60)
    total_rows = 0
    for batch in BATCHES:
        for country in batch:
            if country in all_results:
                info = all_results[country]
                rows = info.get("rows", 0)
                dur = info.get("duration", 0)
                st = info.get("status", "?")
                qf = info.get("q_fail", 0)
                total_rows += rows
                print(f"{country:12s} {rows:>12,} {dur/60:>9.1f}m {st:>10s} {qf:>8d}")
            else:
                print(f"{country:12s} {'N/A':>12s} {'N/A':>10s} {'NOT RUN':>10s}")
    print("-" * 60)
    print(f"{'TOTAL':12s} {total_rows:>12,} {total_elapsed/60:>9.1f}m")
    print(f"\nFinished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
