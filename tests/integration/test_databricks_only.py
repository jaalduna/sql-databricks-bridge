"""Integration tests that only require Databricks access.

These tests can run without SQL Server by mocking the SQL side.
Run with: pytest tests/integration/test_databricks_only.py -v

Requires environment variables:
  - DATABRICKS_HOST
  - DATABRICKS_TOKEN (or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET)
"""

import os
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from tests.conftest import requires_databricks


@requires_databricks
class TestDatabricksConnection:
    """Test Databricks connectivity."""

    def test_databricks_connection(self):
        """Test that we can connect to Databricks."""
        from sql_databricks_bridge.db.databricks import DatabricksClient

        client = DatabricksClient()
        assert client.test_connection() is True

    def test_databricks_current_user(self):
        """Test that we can get current user info."""
        from sql_databricks_bridge.db.databricks import DatabricksClient

        client = DatabricksClient()
        user = client.client.current_user.me()
        assert user is not None
        print(f"Connected as: {user.user_name}")


@requires_databricks
class TestDatabricksFileOperations:
    """Test Databricks file operations (requires a volume)."""

    @pytest.fixture
    def test_volume_path(self):
        """Get test volume path from environment."""
        catalog = os.environ.get("DATABRICKS_CATALOG")
        schema = os.environ.get("DATABRICKS_SCHEMA")
        volume = os.environ.get("DATABRICKS_VOLUME")

        if not all([catalog, schema, volume]):
            pytest.skip(
                "Volume test requires DATABRICKS_CATALOG, DATABRICKS_SCHEMA, "
                "and DATABRICKS_VOLUME environment variables"
            )

        return f"/Volumes/{catalog}/{schema}/{volume}"

    def test_upload_and_download_dataframe(self, test_volume_path, sample_dataframe):
        """Test uploading and downloading a DataFrame."""
        from sql_databricks_bridge.core.uploader import Uploader
        from sql_databricks_bridge.db.databricks import DatabricksClient

        client = DatabricksClient()
        uploader = Uploader(client)

        # Upload
        test_path = f"{test_volume_path}/_test_upload.parquet"
        result = uploader.upload_dataframe(sample_dataframe, test_path)

        assert result.path == test_path
        assert result.rows == len(sample_dataframe)

        # Download and verify
        downloaded = client.download_dataframe(test_path)
        assert len(downloaded) == len(sample_dataframe)

        # Cleanup
        try:
            client.delete_file(test_path)
        except Exception:
            pass  # Ignore cleanup errors

    def test_file_exists(self, test_volume_path):
        """Test checking if file exists."""
        if test_volume_path is None:
            pytest.skip("Volume not configured")

        from sql_databricks_bridge.core.uploader import Uploader
        from sql_databricks_bridge.db.databricks import DatabricksClient

        client = DatabricksClient()
        uploader = Uploader(client)

        # Non-existent file
        assert uploader.file_exists(f"{test_volume_path}/_nonexistent_file.parquet") is False


class TestExtractionWithMockedSQL:
    """Test extraction flow with mocked SQL Server."""

    def test_extraction_with_mock_sql(self, mock_sql_client, tmp_path):
        """Test extraction using mocked SQL Server."""
        from sql_databricks_bridge.core.extractor import Extractor
        from sql_databricks_bridge.core.query_loader import QueryLoader

        # Create test query file
        queries_dir = tmp_path / "queries"
        queries_dir.mkdir()
        (queries_dir / "test_query.sql").write_text(
            "SELECT * FROM {schema}.test_table WHERE country = '{country}'"
        )

        # Create test config file
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "common_params.yaml").write_text("schema: dbo\n")
        (config_dir / "Colombia.yaml").write_text("country: CO\n")

        # Create extractor with mocked SQL client
        extractor = Extractor(
            queries_path=str(queries_dir),
            config_path=str(config_dir),
            sql_client=mock_sql_client,
        )

        # Create job
        job = extractor.create_job(
            country="Colombia",
            destination="/Volumes/test/output",
            queries=["test_query"],
        )

        assert job.job_id is not None
        assert len(job.queries) == 1
        assert "test_query" in job.queries

    def test_query_formatting(self, tmp_path):
        """Test that queries are formatted correctly."""
        from sql_databricks_bridge.core.query_loader import QueryLoader
        from sql_databricks_bridge.core.param_resolver import ParamResolver

        # Create test files
        queries_dir = tmp_path / "queries"
        queries_dir.mkdir()
        (queries_dir / "purchases.sql").write_text(
            "SELECT * FROM {purchases_table} WHERE {flg_scanner} = 1"
        )

        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "common_params.yaml").write_text(
            "flg_scanner: flg_scanner\n"
        )
        (config_dir / "Colombia.yaml").write_text(
            "purchases_table: J_AtosCompra_CO\n"
        )

        # Load and format
        loader = QueryLoader(queries_dir)
        resolver = ParamResolver(config_dir)

        params = resolver.resolve_params("Colombia")
        formatted = loader.format_query("purchases", params)

        assert "J_AtosCompra_CO" in formatted
        assert "flg_scanner = 1" in formatted


class TestSyncWithMockedConnections:
    """Test sync operations with mocked connections."""

    def test_sync_event_processing(self, mock_sql_client, mock_databricks_client, sample_events):
        """Test processing sync events with mocked clients."""
        from sql_databricks_bridge.sync.operations import SyncOperator
        from sql_databricks_bridge.models.events import SyncOperation

        operator = SyncOperator(
            sql_client=mock_sql_client,
            databricks_client=mock_databricks_client,
            max_delete_rows=1000,
        )

        # Test INSERT event
        insert_event = sample_events[0]
        assert insert_event.operation == SyncOperation.INSERT

    def test_validator_functions(self, sample_events):
        """Test sync validators."""
        from sql_databricks_bridge.sync.validators import (
            validate_primary_keys,
            validate_delete_limit,
            ValidationError,
        )

        # INSERT doesn't require PKs
        validate_primary_keys(sample_events[0])

        # UPDATE requires PKs
        validate_primary_keys(sample_events[1])

        # DELETE within limit
        validate_delete_limit(100, 1000, "test")

        # DELETE over limit
        with pytest.raises(ValidationError):
            validate_delete_limit(1001, 1000, "test")


class TestHealthEndpointsWithMocks:
    """Test API health endpoints with mocked connections."""

    def test_liveness_endpoint(self, patch_sql_server, patch_databricks):
        """Test liveness endpoint works without real connections."""
        from fastapi.testclient import TestClient
        from sql_databricks_bridge.main import app

        # Need to skip this if httpx not installed
        pytest.importorskip("httpx")

        client = TestClient(app)
        response = client.get("/health/live")

        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    def test_root_endpoint(self, patch_sql_server, patch_databricks):
        """Test root endpoint."""
        pytest.importorskip("httpx")

        from fastapi.testclient import TestClient
        from sql_databricks_bridge.main import app

        client = TestClient(app)
        response = client.get("/")

        assert response.status_code == 200
        assert "sql-databricks-bridge" in response.json()["service"]
