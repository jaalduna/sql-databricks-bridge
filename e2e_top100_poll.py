import requests
import time
import json
import sys
from datetime import datetime

BASE_URL = "http://127.0.0.1:8000/api/v1"
COUNTRIES = ["argentina", "bolivia", "brasil", "cam", "chile", "colombia", "ecuador", "mexico", "peru"]

# Jobs already triggered - extract job_ids from the events list
jobs = {
    "argentina": "098f764f-4039-417a-8491-72f9f2e7bd45",
    "bolivia":   "79659104-2a89-4f8e-ac6c-a3d21b77cc4b",
    "brasil":    "57e184e6-5086-4f70-9eb1-345f4b9eab75",
    "cam":       "36f1f1de-30ea-4852-9ac4-aa252af464ae",
    "chile":     "705f7cf0-bf32-4909-b28d-e140971abb1f",
    "colombia":  "bcb2fa3e-9b6d-4a3a-9e45-cea01f655bd2",
    "ecuador":   "231b2edf-f410-4c20-817a-aa8a8d54719f",
    "mexico":    "22dac8af-d659-40c7-9f12-5dfd8abb9e0e",
    "peru":      "98fa7b4a-88a7-466a-b1f0-f01422bc369a",
}

print(f"[{datetime.now().strftime('%H:%M:%S')}] Monitoring {len(jobs)} triggered jobs...")
print(f"Query counts: ARG=32, BOL=45, BRA=51, CAM=33, CHI=46, COL=46, ECU=45, MEX=49, PER=45 | Total=392")
print()

poll_count = 0
final_results = {}

while True:
    poll_count += 1
    all_done = True
    ts = datetime.now().strftime('%H:%M:%S')
    
    print(f"[{ts}] === Poll #{poll_count} ===")
    print(f"  {'Country':12s} | {'Status':12s} | {'OK':>4s} | {'Fail':>4s} | {'Run':>4s} | {'Total':>5s} | {'Rows':>10s}")
    print(f"  {'-'*12} | {'-'*12} | {'-'*4} | {'-'*4} | {'-'*4} | {'-'*5} | {'-'*10}")
    
    for country in COUNTRIES:
        job_id = jobs[country]
        try:
            r = requests.get(f"{BASE_URL}/events/{job_id}", timeout=15)
            d = r.json()
            status = d.get("status", "unknown")
            completed = d.get("queries_completed", 0)
            failed = d.get("queries_failed", 0)
            total = d.get("queries_total", 0)
            rows = d.get("total_rows_extracted", 0)
            running = d.get("queries_running", 0)
            
            final_results[country] = d
            
            print(f"  {country:12s} | {status:12s} | {completed:>4d} | {failed:>4d} | {running:>4d} | {total:>5d} | {rows:>10,}")
            
            if status not in ("completed", "failed"):
                all_done = False
        except Exception as e:
            print(f"  {country:12s} | ERROR        |    ? |    ? |    ? |     ? |          ?")
            all_done = False
    
    # Count summary for this poll
    done_count = sum(1 for c in COUNTRIES if final_results.get(c, {}).get("status") in ("completed", "failed"))
    print(f"  >> {done_count}/9 countries finished")
    
    if all_done:
        break
    
    sys.stdout.flush()
    time.sleep(15)

# ============================================================
# FINAL REPORT
# ============================================================
print("\n\n")
print("=" * 110)
print("  FINAL REPORT - TOP 100 Sync Validation Across All 9 Countries")
print("  Stage: inicio | Row Limit: 100 | Date: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
print("=" * 110)
print(f"  {'Country':12s} | {'Status':12s} | {'Completed':>9s} | {'Failed':>6s} | {'Total':>5s} | {'Rows':>10s} | {'Duration':>10s}")
print(f"  {'-'*12} | {'-'*12} | {'-'*9} | {'-'*6} | {'-'*5} | {'-'*10} | {'-'*10}")

sum_rows = 0
sum_completed = 0
sum_failed = 0
sum_total = 0
ok_countries = 0
fail_countries = 0

for country in COUNTRIES:
    d = final_results.get(country, {})
    status = d.get("status", "N/A")
    completed = d.get("queries_completed", 0)
    failed = d.get("queries_failed", 0)
    total = d.get("queries_total", 0)
    rows = d.get("total_rows_extracted", 0)
    
    # Calculate duration if timestamps available
    created = d.get("created_at", "")
    finished = d.get("completed_at", d.get("updated_at", ""))
    duration = ""
    if created and finished:
        try:
            from datetime import datetime as dt2
            t0 = dt2.fromisoformat(created.replace("Z", "+00:00").replace("+00:00", ""))
            t1 = dt2.fromisoformat(finished.replace("Z", "+00:00").replace("+00:00", ""))
            secs = (t1 - t0).total_seconds()
            mins = int(secs // 60)
            sec = int(secs % 60)
            duration = f"{mins}m {sec}s"
        except:
            duration = "?"
    
    sum_rows += rows
    sum_completed += completed
    sum_failed += failed
    sum_total += total
    
    if status == "completed" and failed == 0:
        ok_countries += 1
    elif status == "completed" and failed > 0:
        ok_countries += 1  # completed with some failures
    else:
        fail_countries += 1
    
    print(f"  {country:12s} | {status:12s} | {completed:>9d} | {failed:>6d} | {total:>5d} | {rows:>10,} | {duration:>10s}")

print(f"  {'-'*12} | {'-'*12} | {'-'*9} | {'-'*6} | {'-'*5} | {'-'*10} | {'-'*10}")
print(f"  {'TOTALS':12s} | {'':12s} | {sum_completed:>9d} | {sum_failed:>6d} | {sum_total:>5d} | {sum_rows:>10,} |")
print("=" * 110)

print(f"\n  Countries completed: {ok_countries}/9")
print(f"  Countries failed:    {fail_countries}/9")
print(f"  Queries executed:    {sum_completed + sum_failed}/{sum_total}")
print(f"  Queries succeeded:   {sum_completed}")
print(f"  Queries failed:      {sum_failed}")
print(f"  Total rows:          {sum_rows:,}")

# Detailed failure report
any_failures = False
for country in COUNTRIES:
    d = final_results.get(country, {})
    query_results = d.get("query_results", [])
    failed_queries = [qr for qr in query_results if qr.get("status") == "failed"]
    if failed_queries:
        if not any_failures:
            print("\n" + "-" * 80)
            print("  FAILED QUERIES DETAIL")
            print("-" * 80)
            any_failures = True
        print(f"\n  [{country.upper()}] ({len(failed_queries)} failures):")
        for qr in failed_queries:
            name = qr.get("query_name", "unknown")
            err = qr.get("error", "no error message")
            # Truncate long errors but show enough to be useful
            if len(err) > 150:
                err = err[:150] + "..."
            print(f"    x {name}: {err}")

if not any_failures:
    print("\n  No individual query failures detected across all countries.")

print("\n[E2E Test Complete]")
sys.stdout.flush()
