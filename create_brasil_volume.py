"""Create Unity Catalog volume for Brasil schema.

This creates the necessary volume for storing staging files
when uploading Brasil country-specific data.

Usage:
    python create_brasil_volume.py
"""

import logging
from sql_databricks_bridge.db.databricks import DatabricksClient

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

CATALOG = "000-sql-databricks-bridge"
SCHEMA = "brasil"
VOLUME = "files"


def main():
    # Initialize Databricks client
    databricks_client = DatabricksClient()

    logger.info("Testing Databricks connection...")
    if not databricks_client.test_connection():
        logger.error("Cannot connect to Databricks")
        return 1
    logger.info("✓ Connected to Databricks")
    logger.info("")

    logger.info(f"Creating volume: {CATALOG}.{SCHEMA}.{VOLUME}")

    try:
        # Create schema first (idempotent)
        schema_sql = f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`"
        databricks_client.execute_sql(schema_sql)
        logger.info(f"  ✓ Schema {CATALOG}.{SCHEMA} exists")

        # Create volume
        volume_sql = f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`"
        databricks_client.execute_sql(volume_sql)
        logger.info(f"  ✓ Volume {CATALOG}.{SCHEMA}.{VOLUME} created")

        logger.info("")
        logger.info("=" * 72)
        logger.info("SUCCESS: Brasil volume created!")
        logger.info("=" * 72)
        logger.info("")
        logger.info("Next step: Run Brasil extraction:")
        logger.info("  poetry run sql-databricks-bridge extract \\")
        logger.info("    --queries-path queries \\")
        logger.info("    --country brasil \\")
        logger.info("    --destination 000-sql-databricks-bridge.brasil \\")
        logger.info("    --verbose")

        return 0

    except Exception as e:
        logger.error(f"  ✗ Failed to create volume")
        logger.error(f"    Error: {type(e).__name__}: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
