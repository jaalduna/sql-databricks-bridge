"""Create Unity Catalog volumes for server-level schemas.

This script creates the necessary volumes for storing staging files
when uploading data to server-level schemas like KTCLSQL002.

Usage:
    python create_server_volumes.py --server 2
    python create_server_volumes.py --all
"""

import argparse
import logging
from sql_databricks_bridge.db.databricks import DatabricksClient

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

CATALOG = "000-sql-databricks-bridge"
SERVERS = [1, 2, 3, 4]


def create_volume(databricks_client: DatabricksClient, server_num: int) -> bool:
    """Create volume for a server schema.

    Args:
        databricks_client: Databricks client
        server_num: Server number (1-4)

    Returns:
        True if successful, False otherwise
    """
    schema = f"KTCLSQL00{server_num}"
    volume = "files"

    logger.info(f"Creating volume: {CATALOG}.{schema}.{volume}")

    try:
        # Create schema first (idempotent)
        schema_sql = f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema}`"
        databricks_client.execute_sql(schema_sql)
        logger.info(f"  ✓ Schema {CATALOG}.{schema} exists")

        # Create volume
        volume_sql = f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{schema}`.`{volume}`"
        databricks_client.execute_sql(volume_sql)
        logger.info(f"  ✓ Volume {CATALOG}.{schema}.{volume} created")

        return True

    except Exception as e:
        logger.error(f"  ✗ Failed to create volume for {schema}")
        logger.error(f"    Error: {type(e).__name__}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Create Unity Catalog volumes for server-level schemas"
    )
    parser.add_argument(
        "--server",
        type=int,
        choices=[1, 2, 3, 4],
        help="Server number (1-4)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Create volumes for all servers",
    )

    args = parser.parse_args()

    if not args.server and not args.all:
        parser.error("Either --server or --all must be specified")

    # Initialize Databricks client
    databricks_client = DatabricksClient()

    logger.info("Testing Databricks connection...")
    if not databricks_client.test_connection():
        logger.error("Cannot connect to Databricks")
        return 1
    logger.info("✓ Connected to Databricks")
    logger.info("")

    # Determine which servers to process
    servers_to_process = SERVERS if args.all else [args.server]

    success_count = 0
    failed_count = 0

    for server_num in servers_to_process:
        if create_volume(databricks_client, server_num):
            success_count += 1
        else:
            failed_count += 1
        logger.info("")

    # Summary
    logger.info("=" * 72)
    logger.info(f"SUMMARY: Created {success_count} volumes, {failed_count} failed")
    logger.info("=" * 72)

    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    exit(main())
