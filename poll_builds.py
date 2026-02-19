import json, subprocess, sys, time
from datetime import datetime
TARGET, REPO = "713eb31", "jaalduna/sql-databricks-bridge"
for i in range(1, 25):
    print(f"[{i}] {datetime.now():%H:%M:%S}", flush=True)
    try:
        r = subprocess.run(["curl","-s",f"https://api.github.com/repos/{REPO}/actions/runs?per_page=4"],capture_output=True,text=True,timeout=15)
        runs = json.loads(r.stdout).get("workflow_runs",[])[:4]
        for run in runs:
            print(f"  {run['name']:30s} | {run['status']:12s} | {run.get('conclusion') or '-':10s} | {run['head_sha'][:7]}",flush=True)
        target_runs = [x for x in runs if x["head_sha"][:7].startswith(TARGET)]
        if target_runs and all(x["status"]=="completed" for x in target_runs):
            failed = [x for x in target_runs if x.get("conclusion")!="success"]
            print(f"{'WARNING: '+str(len(failed))+' failed!' if failed else 'All builds succeeded!'}",flush=True)
            break
    except Exception as e:
        print(f"  Error: {e}",flush=True)
    if i<24: time.sleep(30)
print("Done.",flush=True)
