#!/usr/bin/env python3
"""
Verify Delta Sharing setup for sql-databricks-bridge.

This script checks:
1. Share exists
2. Tables are in the share
3. Recipients have access
4. Tables are queryable

Usage:
    python verify_delta_sharing.py --share bolivia_data_share
    python verify_delta_sharing.py --share bolivia_data_share --test-query
"""

import argparse
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

# Load environment
load_dotenv(Path(__file__).parent.parent / ".env")

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "08fa9256f365f47a")


def check_share_exists(client: WorkspaceClient, share_name: str) -> bool:
    """Check if share exists."""
    print(f"\n[1/4] Checking if share '{share_name}' exists...")

    try:
        share = client.shares.get(share_name)
        print(f"  ✓ Share exists")
        print(f"    - Created: {share.created_at}")
        print(f"    - Comment: {share.comment or 'N/A'}")
        return True
    except Exception as e:
        print(f"  ✗ Share not found: {e}")
        return False


def check_share_tables(client: WorkspaceClient, share_name: str) -> int:
    """Check tables in share."""
    print(f"\n[2/4] Checking tables in share '{share_name}'...")

    try:
        share = client.shares.get(share_name)

        if not share.objects:
            print(f"  ⚠ No tables in share")
            return 0

        table_count = len(share.objects)
        print(f"  ✓ Found {table_count} tables:")

        # Show first 10 tables
        for obj in share.objects[:10]:
            print(f"    - {obj.name}")

        if table_count > 10:
            print(f"    ... and {table_count - 10} more")

        return table_count
    except Exception as e:
        print(f"  ✗ Error getting share details: {e}")
        return 0


def check_recipients(client: WorkspaceClient, share_name: str) -> int:
    """Check recipients with access to share."""
    print(f"\n[3/4] Checking recipients with access...")

    try:
        # Get share permissions
        permissions = client.shares.list_permissions(share_name)

        if not permissions or not permissions.privilege_assignments:
            print(f"  ⚠ No recipients have access yet")
            print(f"    Grant access with:")
            print(f"    databricks shares grant --share {share_name} --recipient <recipient_name>")
            return 0

        recipient_count = len(permissions.privilege_assignments)
        print(f"  ✓ {recipient_count} recipient(s) have access:")

        for assignment in permissions.privilege_assignments:
            print(f"    - {assignment.principal}")

        return recipient_count
    except Exception as e:
        print(f"  ⚠ Could not check recipients: {e}")
        print(f"    (This is normal if no recipients exist yet)")
        return 0


def test_query_shared_table(
    client: WorkspaceClient,
    share_name: str,
    sample_table: Optional[str] = None
) -> bool:
    """Test querying a shared table."""
    print(f"\n[4/4] Testing table query...")

    try:
        # Get first table from share
        share = client.shares.get(share_name)

        if not share.objects:
            print(f"  ⚠ No tables to query")
            return False

        # Use provided table or first one
        if sample_table:
            table_name = sample_table
        else:
            table_name = share.objects[0].name

        print(f"  Testing query on: {table_name}")

        # Try to count rows
        sql = f"SELECT COUNT(*) as row_count FROM {table_name}"
        print(f"  SQL: {sql}")

        response = client.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=sql,
            wait_timeout="30s",
        )

        if response.status.state.value == "SUCCEEDED":
            if response.result and response.result.data_array:
                row_count = response.result.data_array[0][0]
                print(f"  ✓ Query successful! Table has {row_count} rows")
                return True
            else:
                print(f"  ✓ Query executed (no results)")
                return True
        else:
            error = response.status.error
            error_msg = error.message if error else "Unknown error"
            print(f"  ✗ Query failed: {error_msg}")
            return False

    except Exception as e:
        print(f"  ✗ Error testing query: {e}")
        return False


def verify_delta_sharing(
    client: WorkspaceClient,
    share_name: str,
    test_query: bool = False
) -> dict:
    """
    Verify Delta Sharing setup.

    Returns:
        Dict with verification results
    """
    print("=" * 70)
    print(f"VERIFYING DELTA SHARING SETUP")
    print(f"Share: {share_name}")
    print("=" * 70)

    results = {
        "share_exists": False,
        "table_count": 0,
        "recipient_count": 0,
        "query_test": False,
    }

    # Check share exists
    results["share_exists"] = check_share_exists(client, share_name)

    if not results["share_exists"]:
        print("\n❌ Share does not exist!")
        print(f"\nCreate it with:")
        print(f"  python scripts/enable_delta_sharing.py --country bolivia --share-name {share_name}")
        return results

    # Check tables
    results["table_count"] = check_share_tables(client, share_name)

    # Check recipients
    results["recipient_count"] = check_recipients(client, share_name)

    # Test query
    if test_query:
        results["query_test"] = test_query_shared_table(client, share_name)

    # Summary
    print("\n" + "=" * 70)
    print("VERIFICATION SUMMARY")
    print("=" * 70)
    print(f"Share exists: {'✓' if results['share_exists'] else '✗'}")
    print(f"Tables in share: {results['table_count']}")
    print(f"Recipients with access: {results['recipient_count']}")
    if test_query:
        print(f"Query test: {'✓' if results['query_test'] else '✗'}")

    # Status
    if results["share_exists"] and results["table_count"] > 0:
        print("\n✅ Delta Sharing is configured correctly!")

        if results["recipient_count"] == 0:
            print("\n📝 Next step: Grant access to recipients")
            print(f"   databricks recipients create --name <recipient_name>")
            print(f"   databricks shares grant --share {share_name} --recipient <recipient_name>")
    else:
        print("\n⚠ Delta Sharing needs configuration")

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Verify Delta Sharing setup"
    )
    parser.add_argument(
        "--share",
        type=str,
        required=True,
        help="Share name to verify (e.g., bolivia_data_share)"
    )
    parser.add_argument(
        "--test-query",
        action="store_true",
        help="Test querying a shared table"
    )

    args = parser.parse_args()

    # Initialize client
    client = WorkspaceClient()

    # Verify
    results = verify_delta_sharing(
        client=client,
        share_name=args.share,
        test_query=args.test_query
    )

    # Exit code
    success = (
        results["share_exists"]
        and results["table_count"] > 0
        and (not args.test_query or results["query_test"])
    )
    exit(0 if success else 1)


if __name__ == "__main__":
    main()
