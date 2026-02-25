import requests
import time
import json
import sys
from datetime import datetime

BASE_URL = "http://127.0.0.1:8000/api/v1"
COUNTRIES = ["argentina", "bolivia", "brasil", "cam", "chile", "colombia", "ecuador", "mexico", "peru"]
STAGE = "inicio"

def trigger_all():
    jobs = {}
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Triggering TOP 100 syncs for {len(COUNTRIES)} countries (stage={STAGE})")
    print("=" * 80)
    for country in COUNTRIES:
        try:
            r = requests.post(
                f"{BASE_URL}/trigger",
                json={"country": country, "stage": STAGE},
                timeout=30
            )
            data = r.json()
            if r.status_code == 200 and "job_id" in data:
                jobs[country] = data["job_id"]
                total = data.get("queries_total", data.get("total_queries", "?"))
                print(f"  {country:12s} -> job_id={data['job_id'][:12]}... | {total} queries queued")
            else:
                print(f"  {country:12s} -> ERROR {r.status_code}: {json.dumps(data)}")
        except Exception as e:
            print(f"  {country:12s} -> EXCEPTION: {e}")
    print("=" * 80)
    return jobs

def poll_status(jobs):
    poll_count = 0
    while True:
        poll_count += 1
        all_done = True
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Poll #{poll_count}")
        print(f"  {'Country':12s} | {'Status':12s} | {'Progress':30s} | {'Rows':>8s}")
        print(f"  {'-'*12} | {'-'*12} | {'-'*30} | {'-'*8}")
        
        results = {}
        for country, job_id in jobs.items():
            try:
                r = requests.get(f"{BASE_URL}/events/{job_id}", timeout=15)
                d = r.json()
                status = d.get("status", "unknown")
                completed = d.get("queries_completed", 0)
                failed = d.get("queries_failed", 0)
                total = d.get("queries_total", 0)
                rows = d.get("total_rows_extracted", 0)
                running = d.get("queries_running", 0)
                
                progress = f"{completed}/{total} ok, {failed} fail, {running} run"
                print(f"  {country:12s} | {status:12s} | {progress:30s} | {rows:>8,}")
                
                results[country] = d
                if status not in ("completed", "failed"):
                    all_done = False
            except Exception as e:
                print(f"  {country:12s} | ERROR        | {str(e)[:30]:30s} |        ?")
                all_done = False
        
        if all_done:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] All jobs finished!")
            return results
        
        sys.stdout.flush()
        time.sleep(15)

def final_report(jobs, results):
    print("\n")
    print("=" * 100)
    print("FINAL REPORT - TOP 100 Sync Validation for All 9 Countries")
    print("=" * 100)
    print(f"  {'Country':12s} | {'Status':12s} | {'Completed':>9s} | {'Failed':>6s} | {'Total':>5s} | {'Rows':>10s} | Notes")
    print(f"  {'-'*12} | {'-'*12} | {'-'*9} | {'-'*6} | {'-'*5} | {'-'*10} | {'-'*20}")
    
    total_rows_all = 0
    total_queries_all = 0
    total_completed_all = 0
    total_failed_all = 0
    countries_ok = 0
    countries_fail = 0
    
    for country in COUNTRIES:
        if country not in results:
            print(f"  {country:12s} | NO DATA      |         - |      - |     - |          - | Not triggered")
            continue
        
        d = results[country]
        status = d.get("status", "unknown")
        completed = d.get("queries_completed", 0)
        failed = d.get("queries_failed", 0)
        total = d.get("queries_total", 0)
        rows = d.get("total_rows_extracted", 0)
        error = d.get("error", "")
        
        total_rows_all += rows
        total_queries_all += total
        total_completed_all += completed
        total_failed_all += failed
        
        if status == "completed":
            countries_ok += 1
        else:
            countries_fail += 1
        
        notes = ""
        if error:
            notes = error[:40]
        elif failed > 0:
            # Check individual query results for errors
            query_results = d.get("query_results", [])
            fail_names = [qr.get("query_name", "?") for qr in query_results if qr.get("status") == "failed"]
            if fail_names:
                notes = f"Failed: {', '.join(fail_names[:3])}"
                if len(fail_names) > 3:
                    notes += f" +{len(fail_names)-3} more"
        
        print(f"  {country:12s} | {status:12s} | {completed:>9d} | {failed:>6d} | {total:>5d} | {rows:>10,} | {notes}")
    
    print(f"  {'-'*12} | {'-'*12} | {'-'*9} | {'-'*6} | {'-'*5} | {'-'*10} |")
    print(f"  {'TOTALS':12s} | {'':12s} | {total_completed_all:>9d} | {total_failed_all:>6d} | {total_queries_all:>5d} | {total_rows_all:>10,} |")
    print("=" * 100)
    print(f"\nSummary: {countries_ok}/9 countries completed successfully, {countries_fail}/9 had failures")
    print(f"Total queries: {total_completed_all + total_failed_all}/{total_queries_all} executed ({total_completed_all} ok, {total_failed_all} failed)")
    print(f"Total rows extracted: {total_rows_all:,}")
    
    # Print detailed failure info
    any_failures = False
    for country in COUNTRIES:
        if country not in results:
            continue
        d = results[country]
        query_results = d.get("query_results", [])
        failed_queries = [qr for qr in query_results if qr.get("status") == "failed"]
        if failed_queries:
            if not any_failures:
                print("\n--- FAILED QUERIES DETAIL ---")
                any_failures = True
            print(f"\n  {country.upper()}:")
            for qr in failed_queries:
                name = qr.get("query_name", "unknown")
                err = qr.get("error", "no error message")
                print(f"    - {name}: {err[:120]}")
    
    if not any_failures:
        print("\nNo individual query failures detected.")
    
    print("\n[Test complete]")

if __name__ == "__main__":
    jobs = trigger_all()
    if not jobs:
        print("ERROR: No jobs were triggered. Aborting.")
        sys.exit(1)
    
    results = poll_status(jobs)
    final_report(jobs, results)
