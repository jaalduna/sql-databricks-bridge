#!/usr/bin/env python3
"""
Enable Delta Sharing for tables in 000-sql-databricks-bridge.bolivia catalog.

Delta Sharing allows secure sharing of Delta Lake tables across organizations
without copying data.

Usage:
    python enable_delta_sharing.py --country bolivia --share-name bolivia_data_share
    python enable_delta_sharing.py --country bolivia --share-name bolivia_data_share --dry-run
    python enable_delta_sharing.py --list-shares
"""

import argparse
import os
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sharing import (
    CreateShare,
    ShareInfo,
    SharedDataObject,
    SharedDataObjectUpdate,
)

# Load environment
load_dotenv(Path(__file__).parent.parent / ".env")

# Configuration
CATALOG = "000-sql-databricks-bridge"
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "08fa9256f365f47a")


def execute_sql(client: WorkspaceClient, sql: str, verbose: bool = True) -> list:
    """Execute SQL statement and return results."""
    if verbose:
        print(f"Executing: {sql[:80]}...")

    response = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout="50s",
    )

    if response.status.state.value == "FAILED":
        error = response.status.error
        error_msg = error.message if error else "Unknown error"
        if verbose:
            print(f"  ERROR: {error_msg}")
        raise Exception(f"SQL execution failed: {error_msg}")

    if verbose:
        print(f"  OK - State: {response.status.state.value}")

    if response.result and response.result.data_array:
        return response.result.data_array
    return []


def list_tables_in_schema(client: WorkspaceClient, schema: str) -> List[str]:
    """List all tables in a schema."""
    print(f"\nListing tables in {CATALOG}.{schema}...")

    try:
        result = execute_sql(
            client,
            f"SHOW TABLES IN `{CATALOG}`.`{schema}`",
            verbose=False
        )

        # Extract table names from result
        tables = []
        for row in result:
            # Row format: [schema_name, table_name, is_temporary]
            if len(row) >= 2:
                tables.append(row[1])

        print(f"  Found {len(tables)} tables")
        return tables
    except Exception as e:
        print(f"  Error listing tables: {e}")
        return []


def create_share(
    client: WorkspaceClient,
    share_name: str,
    comment: Optional[str] = None,
    dry_run: bool = False
) -> Optional[ShareInfo]:
    """Create a Delta Share."""
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Creating share: {share_name}")

    if dry_run:
        print(f"  Would create share '{share_name}'")
        return None

    try:
        # Check if share already exists
        try:
            existing = client.shares.get(share_name)
            print(f"  Share '{share_name}' already exists (created: {existing.created_at})")
            return existing
        except Exception:
            pass  # Share doesn't exist, create it

        share = client.shares.create(
            name=share_name,
            comment=comment or f"Delta Share for {CATALOG} tables"
        )
        print(f"  ✓ Created share: {share.name}")
        return share
    except Exception as e:
        print(f"  ERROR: {e}")
        return None


def add_table_to_share(
    client: WorkspaceClient,
    share_name: str,
    schema: str,
    table_name: str,
    dry_run: bool = False
) -> bool:
    """Add a table to a Delta Share."""
    full_table_name = f"{CATALOG}.{schema}.{table_name}"

    if dry_run:
        print(f"  Would add table: {full_table_name}")
        return True

    try:
        # Update share with new table
        client.shares.update(
            name=share_name,
            updates=[
                SharedDataObjectUpdate(
                    action="ADD",
                    data_object=SharedDataObject(
                        name=full_table_name,
                        data_object_type="TABLE",
                        shared_as=full_table_name,
                    )
                )
            ]
        )
        print(f"    ✓ Added: {table_name}")
        return True
    except Exception as e:
        print(f"    ✗ Error adding {table_name}: {e}")
        return False


def enable_delta_sharing_for_schema(
    client: WorkspaceClient,
    schema: str,
    share_name: str,
    include_tables: Optional[List[str]] = None,
    exclude_tables: Optional[List[str]] = None,
    dry_run: bool = False
) -> dict:
    """
    Enable Delta Sharing for all tables in a schema.

    Args:
        client: Databricks WorkspaceClient
        schema: Schema name (e.g., 'bolivia')
        share_name: Name for the Delta Share
        include_tables: Only include these tables (None = all)
        exclude_tables: Exclude these tables
        dry_run: If True, only print what would be done

    Returns:
        Dict with results summary
    """
    print("=" * 70)
    print(f"Enabling Delta Sharing for: {CATALOG}.{schema}")
    print(f"Share name: {share_name}")
    print(f"Dry run: {dry_run}")
    print("=" * 70)

    # Get all tables in schema
    all_tables = list_tables_in_schema(client, schema)

    if not all_tables:
        print(f"\n⚠ No tables found in {CATALOG}.{schema}")
        return {"success": False, "error": "No tables found"}

    # Filter tables
    tables_to_share = all_tables

    if include_tables:
        tables_to_share = [t for t in tables_to_share if t in include_tables]
        print(f"\nFiltered to {len(tables_to_share)} tables (include list)")

    if exclude_tables:
        tables_to_share = [t for t in tables_to_share if t not in exclude_tables]
        print(f"\nFiltered to {len(tables_to_share)} tables (after exclusions)")

    print(f"\nTables to share ({len(tables_to_share)}):")
    for table in sorted(tables_to_share):
        print(f"  - {table}")

    # Create share
    share = create_share(client, share_name, dry_run=dry_run)

    if not dry_run and not share:
        return {"success": False, "error": "Failed to create share"}

    # Add tables to share
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Adding tables to share...")

    added_count = 0
    failed_count = 0

    for table_name in sorted(tables_to_share):
        success = add_table_to_share(client, share_name, schema, table_name, dry_run)
        if success:
            added_count += 1
        else:
            failed_count += 1

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total tables: {len(all_tables)}")
    print(f"Tables to share: {len(tables_to_share)}")
    print(f"Successfully added: {added_count}")
    print(f"Failed: {failed_count}")

    if not dry_run:
        print(f"\n✓ Delta Share '{share_name}' is ready!")
        print("\nNext steps:")
        print(f"1. Grant access to recipients:")
        print(f"   databricks shares grant --share {share_name} --recipient <recipient_name>")
        print(f"2. Generate share URL:")
        print(f"   databricks shares get --share {share_name}")
        print(f"3. Or use Databricks UI: Data > Delta Sharing > {share_name}")

    return {
        "success": failed_count == 0,
        "share_name": share_name,
        "total_tables": len(all_tables),
        "tables_shared": added_count,
        "failed": failed_count
    }


def list_existing_shares(client: WorkspaceClient):
    """List all existing Delta Shares."""
    print("\n" + "=" * 70)
    print("EXISTING DELTA SHARES")
    print("=" * 70)

    try:
        shares = client.shares.list()

        if not shares:
            print("No shares found")
            return

        for share in shares:
            print(f"\n📊 {share.name}")
            print(f"   Created: {share.created_at}")
            print(f"   Comment: {share.comment or 'N/A'}")

            # Get share details
            try:
                details = client.shares.get(share.name)
                if details.objects:
                    print(f"   Tables ({len(details.objects)}):")
                    for obj in details.objects[:5]:  # Show first 5
                        print(f"     - {obj.name}")
                    if len(details.objects) > 5:
                        print(f"     ... and {len(details.objects) - 5} more")
            except Exception as e:
                print(f"   Error getting details: {e}")

    except Exception as e:
        print(f"Error listing shares: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Enable Delta Sharing for sql-databricks-bridge tables"
    )
    parser.add_argument(
        "--country",
        type=str,
        help="Country schema name (e.g., bolivia, chile, colombia)"
    )
    parser.add_argument(
        "--share-name",
        type=str,
        help="Name for the Delta Share (e.g., bolivia_data_share)"
    )
    parser.add_argument(
        "--include-tables",
        type=str,
        nargs="+",
        help="Only include these tables"
    )
    parser.add_argument(
        "--exclude-tables",
        type=str,
        nargs="+",
        help="Exclude these tables"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes"
    )
    parser.add_argument(
        "--list-shares",
        action="store_true",
        help="List all existing Delta Shares"
    )

    args = parser.parse_args()

    # Initialize client
    client = WorkspaceClient()

    # List shares mode
    if args.list_shares:
        list_existing_shares(client)
        return

    # Validate required args
    if not args.country or not args.share_name:
        parser.error("--country and --share-name are required (or use --list-shares)")

    # Enable Delta Sharing
    result = enable_delta_sharing_for_schema(
        client=client,
        schema=args.country,
        share_name=args.share_name,
        include_tables=args.include_tables,
        exclude_tables=args.exclude_tables,
        dry_run=args.dry_run
    )

    # Exit code
    exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
