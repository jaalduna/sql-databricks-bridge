#!/usr/bin/env python3
"""Test Databricks connection and basic operations.

Run this script to verify your Databricks credentials are working:

    python scripts/test_databricks_connection.py

Requires environment variables:
    DATABRICKS_HOST - Your Databricks workspace URL
    DATABRICKS_TOKEN - Personal access token (or use Service Principal)

Optional:
    DATABRICKS_CATALOG - Unity Catalog name (default: main)
    DATABRICKS_SCHEMA - Schema name (default: default)
    DATABRICKS_VOLUME - Volume name for file tests
"""

import os
import sys
from io import BytesIO

# Add src to path for direct execution
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


def check_env_vars():
    """Check required environment variables."""
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    client_id = os.environ.get("DATABRICKS_CLIENT_ID")

    if not host:
        print("❌ DATABRICKS_HOST not set")
        print("   Set it to your workspace URL, e.g.:")
        print("   export DATABRICKS_HOST=https://your-workspace.azuredatabricks.net")
        return False

    if not token and not client_id:
        print("❌ No authentication configured")
        print("   Set DATABRICKS_TOKEN or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET")
        return False

    print(f"✓ DATABRICKS_HOST: {host}")
    if token:
        print(f"✓ DATABRICKS_TOKEN: {token[:10]}...")
    if client_id:
        print(f"✓ DATABRICKS_CLIENT_ID: {client_id}")

    return True


def test_connection():
    """Test basic Databricks connection."""
    print("\n--- Testing Connection ---")

    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        user = w.current_user.me()
        print(f"✓ Connected as: {user.user_name}")
        return w
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None


def test_list_catalogs(client):
    """Test listing Unity Catalogs."""
    print("\n--- Listing Catalogs ---")

    try:
        catalogs = list(client.catalogs.list())
        print(f"✓ Found {len(catalogs)} catalogs:")
        for cat in catalogs[:5]:
            print(f"   - {cat.name}")
        if len(catalogs) > 5:
            print(f"   ... and {len(catalogs) - 5} more")
        return True
    except Exception as e:
        print(f"⚠ Could not list catalogs: {e}")
        return False


def test_volume_access(client):
    """Test volume access if configured."""
    print("\n--- Testing Volume Access ---")

    catalog = os.environ.get("DATABRICKS_CATALOG")
    schema = os.environ.get("DATABRICKS_SCHEMA")
    volume = os.environ.get("DATABRICKS_VOLUME")

    if not all([catalog, schema, volume]):
        print("⚠ Volume not configured (set DATABRICKS_CATALOG, DATABRICKS_SCHEMA, DATABRICKS_VOLUME)")
        return None

    volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
    print(f"   Testing path: {volume_path}")

    try:
        # Try to list files
        files = list(client.files.list_directory_contents(volume_path))
        print(f"✓ Volume accessible, {len(files)} files found")

        # Try to write a test file
        test_path = f"{volume_path}/_connection_test.txt"
        test_content = b"Connection test successful!"

        client.files.upload(test_path, BytesIO(test_content), overwrite=True)
        print(f"✓ Write test passed: {test_path}")

        # Read it back
        response = client.files.download(test_path)
        content = response.contents.read()
        assert content == test_content
        print("✓ Read test passed")

        # Cleanup
        client.files.delete(test_path)
        print("✓ Cleanup completed")

        return True
    except Exception as e:
        print(f"❌ Volume access failed: {e}")
        return False


def test_sql_execution(client):
    """Test SQL statement execution (requires warehouse)."""
    print("\n--- Testing SQL Execution ---")

    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        print("⚠ DATABRICKS_WAREHOUSE_ID not set, skipping SQL test")
        return None

    try:
        statement = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement="SELECT 1 as test_value",
            wait_timeout="30s",
        )

        if statement.result and statement.result.data_array:
            print(f"✓ SQL execution works: result = {statement.result.data_array[0]}")
            return True
        else:
            print("⚠ SQL executed but no result")
            return False
    except Exception as e:
        print(f"❌ SQL execution failed: {e}")
        return False


def main():
    """Run all connection tests."""
    print("=" * 50)
    print("Databricks Connection Test")
    print("=" * 50)

    # Check environment
    if not check_env_vars():
        print("\n❌ Please set required environment variables")
        sys.exit(1)

    # Test connection
    client = test_connection()
    if not client:
        print("\n❌ Connection failed, cannot continue")
        sys.exit(1)

    # Run additional tests
    results = {
        "catalogs": test_list_catalogs(client),
        "volume": test_volume_access(client),
        "sql": test_sql_execution(client),
    }

    # Summary
    print("\n" + "=" * 50)
    print("Summary")
    print("=" * 50)

    passed = sum(1 for v in results.values() if v is True)
    skipped = sum(1 for v in results.values() if v is None)
    failed = sum(1 for v in results.values() if v is False)

    print(f"✓ Passed: {passed}")
    print(f"⚠ Skipped: {skipped}")
    print(f"❌ Failed: {failed}")

    if failed > 0:
        sys.exit(1)

    print("\n✓ Databricks connection is working!")
    print("\nYou can now run tests with:")
    print("  poetry run pytest tests/ -v")


if __name__ == "__main__":
    main()
