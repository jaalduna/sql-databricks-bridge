"""Trigger j_atoscompra_new diff sync for all 9 countries, 60 periods."""
import json
import time
import requests
from datetime import datetime

API_BASE = "http://127.0.0.1:8000/api/v1"
COUNTRIES = [
    "argentina", "bolivia", "brasil", "cam",
    "chile", "colombia", "ecuador", "mexico", "peru",
]
STAGE = "inicio"
PERIOD = "202602"
LOOKBACK = 60
QUERY = "j_atoscompra_new"

jobs = {}


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
    resp = requests.post(f"{API_BASE}/trigger", json=payload, timeout=30)
    if resp.status_code == 201:
        data = resp.json()
        return data["job_id"]
    else:
        print(f"  FAILED {country}: {resp.status_code} {resp.text[:200]}")
        return None


def check_job(job_id):
    resp = requests.get(f"{API_BASE}/events/{job_id}", timeout=10)
    if resp.status_code != 200:
        return None
    return resp.json()


# Trigger all countries
print(f"=== Triggering {len(COUNTRIES)} countries at {datetime.now().strftime('%H:%M:%S')} ===")
print(f"    Query: {QUERY}, Lookback: {LOOKBACK} months, Diff sync: ON")
print()

for country in COUNTRIES:
    job_id = trigger(country)
    if job_id:
        jobs[country] = {"job_id": job_id, "start": time.time()}
        print(f"  {country}: triggered (job {job_id[:8]}...)")
    time.sleep(0.5)  # small delay between triggers

print(f"\n=== Monitoring {len(jobs)} jobs ===\n")

# Poll until all done
completed = set()
while len(completed) < len(jobs):
    time.sleep(10)
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
        running = data.get("running_queries", [])
        current_step = data.get("current_step", "")

        elapsed = time.time() - info["start"]

        if q_total > 0 and (q_done + q_fail) >= q_total and q_run == 0:
            # All queries finished
            completed.add(country)
            info["rows"] = rows
            info["duration"] = elapsed
            info["status"] = "ok" if q_fail == 0 else f"{q_fail} failed"
            info["step"] = current_step
            print(f"  [{now}] {country:12s}: DONE  {rows:>12,} rows  {elapsed:>7.1f}s  step={current_step}")
        elif status in ("completed", "failed", "cancelled"):
            completed.add(country)
            info["rows"] = rows
            info["duration"] = elapsed
            info["status"] = status
            info["step"] = current_step
            err = (data.get("error") or "")[:80]
            print(f"  [{now}] {country:12s}: {status:8s} {rows:>12,} rows  {elapsed:>7.1f}s  {err}")
        else:
            rq = ",".join(running) if running else "-"
            print(f"  [{now}] {country:12s}: {status:8s} {q_done}/{q_total} done  {rows:>10,} rows  {elapsed:>6.0f}s  running=[{rq}]  step={current_step}")

print(f"\n=== SUMMARY ===\n")
print(f"{'Country':12s} {'Rows':>12s} {'Duration':>10s} {'Status':>10s} {'Step':>20s}")
print("-" * 70)
total_rows = 0
total_time = 0
for country in COUNTRIES:
    if country in jobs:
        info = jobs[country]
        rows = info.get("rows", 0)
        dur = info.get("duration", 0)
        st = info.get("status", "?")
        step = info.get("step", "")
        total_rows += rows
        total_time = max(total_time, dur)
        print(f"{country:12s} {rows:>12,} {dur:>9.1f}s {st:>10s} {step:>20s}")
print("-" * 70)
print(f"{'TOTAL':12s} {total_rows:>12,} {total_time:>9.1f}s")
