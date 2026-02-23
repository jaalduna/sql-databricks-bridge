from sql_databricks_bridge.db.databricks import DatabricksClient
dbx = DatabricksClient()
try:
    dbx.execute_sql("DROP TABLE IF EXISTS `000-sql-databricks-bridge`.`bolivia`.`_diff_sync_test`")
    print("Table dropped")
except Exception as e:
    print(f"drop: {e}")
try:
    dbx.execute_sql("DELETE FROM `000-sql-databricks-bridge`.events.sync_fingerprints WHERE country = 'bolivia' AND table_name = '_diff_sync_test'")
    print("Fingerprints deleted")
except Exception as e:
    print(f"delete fp: {e}")
print("Cleanup done")
