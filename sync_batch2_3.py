"""Trigger batches 2 and 3 (skip batch 1 which is done/running).

Batch 2: argentina, chile, cam
Batch 3: mexico, brasil, colombia
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
POLL_INTERVAL = 30

BATCHES = [
    ["argentina", "chile", "cam"],
    ["mexico", "brasil", "colombia"],
]


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
            return resp.json()["job_id"]
        else:
            print(f"  FAILED {country}: {resp.status_code} {resp.text[:200]}")
            return None
    except Exception as e:
        print(f"  ERROR {country}: {e}")
        return None


def check_job(job_id):
    try:
        resp = requests.get(f"{API_BASE}/events/{job_id}", timeout=10)
        if resp.status_code != 200:
            return None
        return resp.json()
    except:
        return None


def is_done(data):
    if not data:
        return False
    if data.get("status") in ("completed", "failed", "cancelled"):
        return True
    qt = data.get("queries_total", 0)
    qd = data.get("queries_completed", 0)
    qf = data.get("queries_failed", 0)
    qr = data.get("queries_running", 0)
    return qt > 0 and (qd + qf) >= qt and qr == 0


def run_batch(num, countries, results):
    print(f"\n{'='*60}")
    print(f"  BATCH {num}: {', '.join(countries)}")
    print(f"  {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*60}\n")

    jobs = {}
    for c in countries:
        jid = trigger(c)
        if jid:
            jobs[c] = {"job_id": jid, "start": time.time()}
            print(f"  {c}: triggered ({jid[:8]}...)")
        time.sleep(1)

    done = set()
    while len(done) < len(jobs):
        time.sleep(POLL_INTERVAL)
        now = datetime.now().strftime("%H:%M:%S")
        for c, info in jobs.items():
            if c in done:
                continue
            d = check_job(info["job_id"])
            if not d:
                continue
            rows = d.get("total_rows_extracted", 0)
            step = d.get("current_step", "")
            elapsed = time.time() - info["start"]
            if is_done(d):
                done.add(c)
                info.update(rows=rows, duration=elapsed, status=d["status"], step=step)
                results[c] = info
                print(f"  [{now}] {c:12s}: DONE  {rows:>12,} rows  {elapsed/60:>6.1f}min  step={step}")
            else:
                qd = d.get("queries_completed", 0)
                qt = d.get("queries_total", 0)
                print(f"  [{now}] {c:12s}: {d['status']:8s} {qd}/{qt}  {rows:>10,} rows  {elapsed/60:>5.1f}min  step={step}")

    print(f"\n  Batch {num} done at {datetime.now().strftime('%H:%M:%S')}")


def main():
    t0 = time.time()
    results = {}

    print(f"{'='*60}")
    print(f"  BATCHES 2-3: 6 countries")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    for i, batch in enumerate(BATCHES, 2):
        run_batch(i, batch, results)
        if i < len(BATCHES) + 1:
            print(f"\n  Waiting 10s...")
            time.sleep(10)

    total = time.time() - t0
    print(f"\n{'='*60}")
    print(f"  SUMMARY")
    print(f"{'='*60}\n")
    print(f"{'Country':12s} {'Rows':>12s} {'Duration':>10s} {'Status':>10s}")
    print("-" * 50)
    tr = 0
    for batch in BATCHES:
        for c in batch:
            if c in results:
                r = results[c]
                rows = r.get("rows", 0)
                dur = r.get("duration", 0)
                tr += rows
                print(f"{c:12s} {rows:>12,} {dur/60:>9.1f}m {r.get('status','?'):>10s}")
    print("-" * 50)
    print(f"{'TOTAL':12s} {tr:>12,} {total/60:>9.1f}m")


if __name__ == "__main__":
    main()
