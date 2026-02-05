"""Setup Databricks catalog, schema, and event tables for integration testing."""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load environment
load_dotenv(Path(__file__).parent.parent / ".env")

from databricks.sdk import WorkspaceClient

# Configuration
CATALOG = "000-sql-databricks-bridge"
SCHEMA = "events"
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "08fa9256f365f47a")


def execute_sql(client: WorkspaceClient, sql: str) -> list:
    """Execute SQL statement and return results."""
    print(f"Executing: {sql[:100]}...")
    response = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout="50s",
    )

    if response.status.state.value == "FAILED":
        error = response.status.error
        print(f"  ERROR: {error.message if error else 'Unknown error'}")
        return []

    print(f"  OK - State: {response.status.state.value}")

    if response.result and response.result.data_array:
        return response.result.data_array
    return []


def main():
    print("=" * 60)
    print("Setting up Databricks tables for sql-databricks-bridge")
    print("=" * 60)

    client = WorkspaceClient()

    # Step 1: Create catalog
    print(f"\n[1/5] Creating catalog: {CATALOG}")
    execute_sql(client, f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")

    # Step 2: Create schema
    print(f"\n[2/5] Creating schema: {CATALOG}.{SCHEMA}")
    execute_sql(client, f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")

    # Step 3: Create bridge_events table
    print(f"\n[3/5] Creating bridge_events table")
    execute_sql(client, f"""
        CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.bridge_events (
            event_id STRING NOT NULL,
            operation STRING NOT NULL,
            source_table STRING NOT NULL,
            target_table STRING NOT NULL,
            primary_keys ARRAY<STRING>,
            priority INT,
            status STRING,
            rows_expected INT,
            rows_affected INT,
            discrepancy INT,
            warning STRING,
            error_message STRING,
            retry_count INT,
            max_retries INT,
            filter_conditions STRING,
            metadata STRING,
            created_at TIMESTAMP,
            processed_at TIMESTAMP
        )
        USING DELTA
    """)

    # Step 4: Create test source table (for sync tests)
    print(f"\n[4/5] Creating test_market_scope_data table (source for sync)")
    execute_sql(client, f"""
        CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.test_market_scope_data (
            id INT NOT NULL,
            username STRING NOT NULL,
            is_user INT,
            is_admin INT,
            test_marker STRING
        )
        USING DELTA
    """)

    # Step 5: Verify tables
    print(f"\n[5/5] Verifying tables")
    tables = execute_sql(client, f"SHOW TABLES IN `{CATALOG}`.`{SCHEMA}`")
    print(f"\nTables in {CATALOG}.{SCHEMA}:")
    for table in tables:
        print(f"  - {table}")

    print("\n" + "=" * 60)
    print("Setup complete!")
    print("=" * 60)

    # Print summary
    print(f"""
Configuration summary:
  - Catalog: {CATALOG}
  - Schema: {SCHEMA}
  - Event table: {CATALOG}.{SCHEMA}.bridge_events
  - Test data table: {CATALOG}.{SCHEMA}.test_market_scope_data
  - Warehouse ID: {WAREHOUSE_ID}

Update your .env with:
  DATABRICKS_CATALOG="{CATALOG}"
  DATABRICKS_SCHEMA="{SCHEMA}"
""")


if __name__ == "__main__":
    main()
