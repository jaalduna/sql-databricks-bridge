"""Cancel all active Databricks job runs."""
from sql_databricks_bridge.db.databricks import DatabricksClient

dbx = DatabricksClient()

# List active runs
print("Listing active runs...")
active_runs = list(dbx.client.jobs.list_runs(active_only=True))
print(f"Found {len(active_runs)} active runs")

for run in active_runs:
    run_id = run.run_id
    job_name = run.run_name or "unknown"
    state = run.state.life_cycle_state if run.state else "?"
    print(f"  Cancelling run {run_id} ({job_name}) state={state}")
    try:
        dbx.client.jobs.cancel_run(run_id)
        print(f"    -> Cancelled")
    except Exception as e:
        print(f"    -> Failed: {e}")

print("Done.")
