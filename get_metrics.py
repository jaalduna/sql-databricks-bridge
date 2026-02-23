"""Fetch metrics from completed Bolivia diff sync jobs."""
import json
import requests

API_BASE = "http://127.0.0.1:8001/api/v1"

# Get all recent Bolivia events
resp = requests.get(f"{API_BASE}/events", params={"country": "bolivia", "limit": 10})
events = resp.json()["items"]

for e in events:
    # Get detail for each
    detail = requests.get(f"{API_BASE}/events/{e['job_id']}").json()
    stage = detail.get("stage", "?")
    period = detail.get("period", "")
    status = detail["status"]
    tag = detail.get("tag", "")
    total_rows = detail.get("total_rows_extracted", 0)
    queries_total = detail.get("queries_total", 0)
    queries_completed = detail.get("queries_completed", 0)
    queries_failed = detail.get("queries_failed", 0)
    created = detail.get("created_at", "")[:19]

    print(f"\nJob: {e['job_id'][:8]}... | {created} | {stage}/{period} | {status}")
    print(f"  Queries: {queries_completed}/{queries_total} ok, {queries_failed} failed")
    print(f"  Total rows: {total_rows:,}")

    for r in detail.get("results", []):
        name = r.get("query_name", "?")
        rstatus = r.get("status", "?")
        rows = r.get("rows_extracted", 0)
        dur = r.get("duration_seconds", 0)
        err = r.get("error", "")
        if err:
            err = err[:120]
        print(f"    {name}: {rstatus}, {rows:,} rows, {dur:.1f}s {err}")
