"""Pytest configuration and fixtures.

This module provides fixtures for testing with mocked SQL Server
and real Databricks connections.
"""

import os
from unittest.mock import MagicMock, patch

import polars as pl
import pytest


# Check if we have real Databricks access
DATABRICKS_AVAILABLE = bool(
    os.environ.get("DATABRICKS_HOST") and
    (os.environ.get("DATABRICKS_TOKEN") or os.environ.get("DATABRICKS_CLIENT_ID"))
)

# Skip marker for tests requiring real Databricks
requires_databricks = pytest.mark.skipif(
    not DATABRICKS_AVAILABLE,
    reason="Requires DATABRICKS_HOST and DATABRICKS_TOKEN environment variables"
)


@pytest.fixture
def mock_sql_client():
    """Create a mocked SQL Server client.

    Use this fixture when you don't have SQL Server access.
    """
    mock_client = MagicMock()

    # Mock test_connection to return True
    mock_client.test_connection.return_value = True

    # Mock execute_query to return sample DataFrame
    mock_client.execute_query.return_value = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
        "value": [100, 200, 300],
    })

    # Mock execute_query_chunked to yield chunks
    def mock_chunked(query, params=None, chunk_size=100_000):
        yield pl.DataFrame({
            "id": [1, 2],
            "name": ["a", "b"],
        })
        yield pl.DataFrame({
            "id": [3, 4],
            "name": ["c", "d"],
        })

    mock_client.execute_query_chunked = mock_chunked

    # Mock execute_write to return affected rows
    mock_client.execute_write.return_value = 5

    # Mock bulk_insert
    mock_client.bulk_insert.return_value = 100

    # Mock close
    mock_client.close.return_value = None

    return mock_client


@pytest.fixture
def mock_databricks_client():
    """Create a mocked Databricks client.

    Use this fixture for unit tests that don't need real Databricks.
    """
    mock_client = MagicMock()

    # Mock test_connection
    mock_client.test_connection.return_value = True

    # Mock file operations
    mock_client.upload_dataframe.return_value = "/Volumes/test/path/file.parquet"
    mock_client.file_exists = MagicMock(return_value=False)
    mock_client.download_dataframe.return_value = pl.DataFrame({"col": [1, 2, 3]})

    # Mock execute_sql
    mock_client.execute_sql.return_value = [
        {"event_id": "1", "operation": "INSERT", "status": "pending"},
    ]

    # Mock the inner client
    mock_client.client = MagicMock()
    mock_client.client.files.upload.return_value = None
    mock_client.client.files.get_status.return_value = MagicMock()
    mock_client.client.current_user.me.return_value = MagicMock(user_name="test_user")

    return mock_client


@pytest.fixture
def sample_dataframe():
    """Create a sample Polars DataFrame for testing."""
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "country": ["CO", "CO", "BR", "MX", "AR"],
        "value": [100.5, 200.0, 150.25, 300.0, 250.75],
    })


@pytest.fixture
def sample_events():
    """Create sample sync events for testing."""
    from sql_databricks_bridge.models.events import SyncEvent, SyncOperation

    return [
        SyncEvent(
            event_id="evt-001",
            operation=SyncOperation.INSERT,
            source_table="catalog.schema.source_table",
            target_table="dbo.target_table",
            priority=1,
        ),
        SyncEvent(
            event_id="evt-002",
            operation=SyncOperation.UPDATE,
            source_table="catalog.schema.updates",
            target_table="dbo.target_table",
            primary_keys=["id"],
            priority=2,
        ),
        SyncEvent(
            event_id="evt-003",
            operation=SyncOperation.DELETE,
            source_table="catalog.schema.deletions",
            target_table="dbo.target_table",
            primary_keys=["id"],
            filter_conditions={"status": "inactive"},
            priority=0,
        ),
    ]


@pytest.fixture
def patch_sql_server():
    """Patch SQL Server client globally for tests."""
    with patch("sql_databricks_bridge.db.sql_server.SQLServerClient") as mock:
        mock_instance = MagicMock()
        mock_instance.test_connection.return_value = True
        mock_instance.execute_query.return_value = pl.DataFrame()
        mock.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def patch_databricks():
    """Patch Databricks client globally for tests."""
    with patch("sql_databricks_bridge.db.databricks.DatabricksClient") as mock:
        mock_instance = MagicMock()
        mock_instance.test_connection.return_value = True
        mock.return_value = mock_instance
        yield mock_instance
