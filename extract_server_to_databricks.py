"""
Extract PS_LATAM Tables from KTCLSQL Servers to Databricks

This script properly extracts loc_psdata_compras and loc_psdata_procesado from
each KTCLSQL server to Databricks Unity Catalog using the full extraction pipeline.

Usage:
    python extract_server_to_databricks.py --server 2                    # KTCLSQL002
    python extract_server_to_databricks.py --server 3 --limit 1000       # Test with limit
    python extract_server_to_databricks.py --server 2 --tables loc_psdata_compras  # Single table
"""

import sys
import argparse
import logging
from pathlib import Path
import yaml
from sql_databricks_bridge.db.sql_server import SQLServerClient
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.core.delta_writer import DeltaTableWriter

# Configuration
SERVERS = {
    1: "KTCLSQL001.KT.group.local",
    2: "KTCLSQL002.KT.group.local",
    3: "KTCLSQL003.KT.group.local",
    4: "KTCLSQL004.KT.group.local",
}

CATALOG = "000-sql-databricks-bridge"
DATABASE = "PS_LATAM"
TABLES = ["loc_psdata_compras", "loc_psdata_procesado"]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_server_config(server_num: int) -> dict:
    """Load server configuration from YAML file."""
    config_file = Path(f"config/ktclsql00{server_num}.yaml")
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_file}")

    with open(config_file) as f:
        return yaml.safe_load(f)


def extract_server_tables(
    server_num: int,
    tables: list[str] = None,
    limit: int = None,
    chunk_size: int = 100_000,
    lookback_months: int = 24,
) -> dict:
    """Extract tables from a server to Databricks."""

    server_name = SERVERS[server_num]
    schema = f"KTCLSQL00{server_num}"
    queries_path = Path(f"queries/servers/KTCLSQL00{server_num}")

    if tables is None:
        tables = TABLES

    logger.info(f"{'=' * 72}")
    logger.info(f"Starting extraction from {server_name}")
    logger.info(f"  Database: {DATABASE}")
    logger.info(f"  Target:   {CATALOG}.{schema}")
    logger.info(f"  Tables:   {', '.join(tables)}")
    logger.info(f"{'=' * 72}")

    # Load server config
    try:
        config = load_server_config(server_num)
        logger.info(f"Loaded config from config/ktclsql00{server_num}.yaml")
    except FileNotFoundError as e:
        logger.error(str(e))
        return {"success": 0, "failed": len(tables)}

    # Initialize clients
    sql_client = SQLServerClient(server=server_name, database=DATABASE)
    databricks_client = DatabricksClient()

    # Test connections
    logger.info("Testing SQL Server connection...")
    if not sql_client.test_connection():
        logger.error(f"Cannot connect to {server_name}/{DATABASE}")
        return {"success": 0, "failed": len(tables)}
    logger.info(f"✓ Connected to {server_name}/{DATABASE}")

    logger.info("Testing Databricks connection...")
    if not databricks_client.test_connection():
        logger.error("Cannot connect to Databricks")
        sql_client.close()
        return {"success": 0, "failed": len(tables)}
    logger.info("✓ Connected to Databricks")

    # Initialize writer
    writer = DeltaTableWriter(databricks_client)

    # Fake country name for server-level queries (used by write_dataframe for naming)
    pseudo_country = f"ktclsql00{server_num}"

    # Target catalog and schema
    target_catalog = config["destination"]["catalog"]
    target_schema = config["destination"]["schema"]

    success_count = 0
    failed_count = 0

    for table_name in tables:
        logger.info(f"\n{'=' * 72}")
        logger.info(f"Extracting table: {table_name}")
        logger.info(f"{'=' * 72}")

        query_file = queries_path / f"{table_name}.sql"
        if not query_file.exists():
            logger.error(f"Query file not found: {query_file}")
            failed_count += 1
            continue

        try:
            # Read query SQL directly from file
            query_sql = query_file.read_text(encoding="utf-8")

            # Substitute lookback_months placeholder
            query_sql = query_sql.replace("{lookback_months}", str(lookback_months))

            # Apply row limit if specified (SQL Server TOP syntax)
            if limit is not None and limit > 0:
                query_sql = f"SELECT TOP {limit} * FROM ({query_sql}) AS _limited_subquery"
                logger.info(f"  Query limited to {limit:,} rows")

            # Execute query in chunks
            import polars as pl

            total_rows = 0
            all_chunks = []

            for chunk_df in sql_client.execute_query_chunked(query_sql, chunk_size=chunk_size):
                rows_in_chunk = len(chunk_df)
                total_rows += rows_in_chunk
                logger.info(f"  Extracted chunk: {rows_in_chunk:,} rows (Total: {total_rows:,})")
                all_chunks.append(chunk_df)

            if all_chunks:
                combined = pl.concat(all_chunks)

                logger.info(
                    f"  Writing {total_rows:,} rows to {target_catalog}.{target_schema}.{table_name}"
                )

                # Write to Databricks using write_dataframe
                result = writer.write_dataframe(
                    combined,
                    table_name,
                    pseudo_country,
                    catalog=target_catalog,
                    schema=target_schema,
                )

                logger.info(
                    f"✓ SUCCESS: {table_name} - {result.rows:,} rows -> {result.table_name}"
                )
                success_count += 1
            else:
                logger.info(f"✓ SUCCESS: {table_name} - 0 rows (empty result)")
                success_count += 1

        except Exception as e:
            logger.error(f"✗ FAILED: {table_name}")
            logger.error(f"  Error: {type(e).__name__}: {e}")
            import traceback

            logger.debug(traceback.format_exc())
            failed_count += 1

    # Cleanup
    sql_client.close()

    logger.info(f"\n{'=' * 72}")
    logger.info(f"EXTRACTION SUMMARY for KTCLSQL00{server_num}")
    logger.info(f"{'=' * 72}")
    logger.info(f"  Successful: {success_count}")
    logger.info(f"  Failed:     {failed_count}")
    logger.info(f"{'=' * 72}")

    return {"success": success_count, "failed": failed_count}


def main():
    parser = argparse.ArgumentParser(
        description="Extract PS_LATAM tables from KTCLSQL servers to Databricks"
    )
    parser.add_argument(
        "--server",
        type=int,
        required=True,
        choices=[1, 2, 3, 4],
        help="Server number (1-4)",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=TABLES,
        help=f"Specific tables to extract (default: all - {', '.join(TABLES)})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit rows per query (for testing)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100_000,
        help="Rows per extraction chunk (default: 100,000)",
    )
    parser.add_argument(
        "--lookback-months",
        type=int,
        default=24,
        help="Number of months to look back for fact queries (default: 24)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Run extraction
    result = extract_server_tables(
        server_num=args.server,
        tables=args.tables,
        limit=args.limit,
        chunk_size=args.chunk_size,
        lookback_months=args.lookback_months,
    )

    # Exit with appropriate code
    if result["failed"] > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
