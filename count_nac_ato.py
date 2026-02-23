"""Quick count check for CAM nac_ato with different lookback values."""
import sys
from sql_databricks_bridge.db.sql_server import SQLServerClient

client = SQLServerClient()

print("Counting CAM_NAC.dbo.nac_ato...", flush=True)

r1 = client.execute_query("SELECT COUNT(*) AS cnt FROM CAM_NAC.dbo.nac_ato")
print(f"Total:     {int(r1.item(0, 0)):>15,}", flush=True)

r2 = client.execute_query(
    "SELECT COUNT(*) AS cnt FROM CAM_NAC.dbo.nac_ato "
    "WHERE dataColeta >= DATEADD(MONTH, -1, GETDATE())"
)
print(f"1-month:   {int(r2.item(0, 0)):>15,}", flush=True)

r3 = client.execute_query(
    "SELECT COUNT(*) AS cnt FROM CAM_NAC.dbo.nac_ato "
    "WHERE dataColeta >= DATEADD(MONTH, -13, GETDATE())"
)
print(f"13-month:  {int(r3.item(0, 0)):>15,}", flush=True)
