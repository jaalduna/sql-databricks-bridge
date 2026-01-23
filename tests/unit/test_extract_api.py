"""Unit tests for extraction API endpoints."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import yaml
from fastapi.testclient import TestClient

from sql_databricks_bridge.api.routes.extract import get_extractor
from sql_databricks_bridge.api.schemas import ExtractionRequest


@pytest.fixture
def sample_queries_dir():
    """Create temp directory with sample SQL queries."""
    with tempfile.TemporaryDirectory() as tmpdir:
        query1 = Path(tmpdir) / "query1.sql"
        query1.write_text("SELECT * FROM {table1}")

        yield tmpdir


@pytest.fixture
def sample_config_dir():
    """Create temp directory with sample config."""
    with tempfile.TemporaryDirectory() as tmpdir:
        common_config = Path(tmpdir) / "common_params.yaml"
        common_config.write_text(yaml.dump({"table1": "dbo.users"}))

        cl_config = Path(tmpdir) / "CL.yaml"
        cl_config.write_text(yaml.dump({"country": "CL"}))

        yield tmpdir


@pytest.fixture
def client():
    """Create test client."""
    with patch("sql_databricks_bridge.main._event_poller", None):
        from sql_databricks_bridge.main import app

        yield TestClient(app)


class TestGetExtractor:
    """Tests for get_extractor function."""

    def test_creates_extractor(self, sample_queries_dir, sample_config_dir):
        """Create extractor from request."""
        with patch(
            "sql_databricks_bridge.api.routes.extract.SQLServerClient"
        ) as mock_sql:
            mock_sql.return_value = MagicMock()

            request = ExtractionRequest(
                country="CL",
                destination="/volumes/test",
                queries_path=sample_queries_dir,
                config_path=sample_config_dir,
            )

            extractor = get_extractor(request)

            assert extractor is not None
            mock_sql.assert_called_once()


class TestStartExtraction:
    """Tests for POST /extract endpoint."""

    def test_start_extraction_success(
        self, client, sample_queries_dir, sample_config_dir
    ):
        """Start extraction job successfully."""
        with patch(
            "sql_databricks_bridge.api.routes.extract.SQLServerClient"
        ) as mock_sql:
            with patch(
                "sql_databricks_bridge.api.routes.extract.DatabricksClient"
            ) as mock_db:
                mock_sql.return_value = MagicMock()
                mock_db.return_value = MagicMock()

                response = client.post(
                    "/extract",
                    json={
                        "country": "CL",
                        "destination": "/volumes/test",
                        "queries_path": sample_queries_dir,
                        "config_path": sample_config_dir,
                        "queries": ["query1"],
                    },
                )

        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "pending"
        assert data["job_id"] is not None

    def test_start_extraction_invalid_query(
        self, client, sample_queries_dir, sample_config_dir
    ):
        """Reject unknown queries."""
        with patch(
            "sql_databricks_bridge.api.routes.extract.SQLServerClient"
        ) as mock_sql:
            mock_sql.return_value = MagicMock()

            response = client.post(
                "/extract",
                json={
                    "country": "CL",
                    "destination": "/volumes/test",
                    "queries_path": sample_queries_dir,
                    "config_path": sample_config_dir,
                    "queries": ["nonexistent"],
                },
            )

        assert response.status_code == 400

    def test_start_extraction_missing_path(self, client):
        """Reject missing query path."""
        response = client.post(
            "/extract",
            json={
                "country": "CL",
                "destination": "/volumes/test",
                "queries_path": "/nonexistent/path",
                "config_path": "/nonexistent/config",
            },
        )

        assert response.status_code in (404, 500)


class TestRunExtractionJob:
    """Tests for background extraction job."""

    @pytest.mark.asyncio
    async def test_run_extraction_success(self, sample_queries_dir, sample_config_dir):
        """Run extraction job to completion."""
        from sql_databricks_bridge.api.routes.extract import run_extraction_job
        from sql_databricks_bridge.api.schemas import JobStatus
        from sql_databricks_bridge.core.extractor import ExtractionJob

        # Create mock extractor
        mock_extractor = MagicMock()
        chunk = pl.DataFrame({"id": [1], "name": ["test"]})
        mock_extractor.execute_query.return_value = iter([chunk])

        # Create mock uploader
        mock_uploader = MagicMock()
        mock_uploader.file_exists.return_value = False

        # Create job
        job = ExtractionJob(
            job_id="test-123",
            country="CL",
            destination="/volumes/test",
            queries=["query1"],
            chunk_size=1000,
        )

        await run_extraction_job(
            mock_extractor,
            job,
            mock_uploader,
            overwrite=True,
        )

        assert job.status == JobStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_run_extraction_skips_existing(self):
        """Skip existing files when not overwriting."""
        from sql_databricks_bridge.api.routes.extract import run_extraction_job
        from sql_databricks_bridge.api.schemas import JobStatus
        from sql_databricks_bridge.core.extractor import ExtractionJob

        mock_extractor = MagicMock()
        mock_uploader = MagicMock()
        mock_uploader.file_exists.return_value = True  # File exists

        job = ExtractionJob(
            job_id="test-123",
            country="CL",
            destination="/volumes/test",
            queries=["query1"],
            chunk_size=1000,
        )

        await run_extraction_job(
            mock_extractor,
            job,
            mock_uploader,
            overwrite=False,  # Don't overwrite
        )

        # Should complete without calling execute_query
        assert job.status == JobStatus.COMPLETED
        mock_extractor.execute_query.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_extraction_handles_error(self):
        """Handle extraction errors."""
        from sql_databricks_bridge.api.routes.extract import run_extraction_job
        from sql_databricks_bridge.api.schemas import JobStatus
        from sql_databricks_bridge.core.extractor import ExtractionJob

        mock_extractor = MagicMock()
        mock_extractor.execute_query.side_effect = Exception("Query failed")

        mock_uploader = MagicMock()
        mock_uploader.file_exists.return_value = False

        job = ExtractionJob(
            job_id="test-123",
            country="CL",
            destination="/volumes/test",
            queries=["query1"],
            chunk_size=1000,
        )

        await run_extraction_job(
            mock_extractor,
            job,
            mock_uploader,
            overwrite=True,
        )

        assert job.status == JobStatus.FAILED
        assert "Query failed" in job.error
