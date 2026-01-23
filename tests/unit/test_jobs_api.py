"""Unit tests for jobs API endpoints."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from sql_databricks_bridge.api.schemas import JobStatus


@pytest.fixture
def client():
    """Create test client."""
    with patch("sql_databricks_bridge.main._event_poller", None):
        from sql_databricks_bridge.main import app

        yield TestClient(app)


@pytest.fixture
def mock_extractor():
    """Create mock extractor with a job."""
    from sql_databricks_bridge.core.extractor import ExtractionJob

    job = ExtractionJob(
        job_id="test-job-123",
        country="CL",
        destination="/volumes/test",
        queries=["query1", "query2"],
        chunk_size=1000,
        status=JobStatus.RUNNING,
    )

    extractor = MagicMock()
    extractor.get_job.return_value = job

    return extractor, job


class TestGetJobStatus:
    """Tests for GET /jobs/{job_id} endpoint."""

    def test_get_job_status_success(self, client, mock_extractor):
        """Get status of existing job."""
        extractor, job = mock_extractor

        with patch(
            "sql_databricks_bridge.api.routes.jobs._extractors",
            {job.job_id: extractor},
        ):
            response = client.get(f"/jobs/{job.job_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["job_id"] == job.job_id
        assert data["country"] == "CL"
        assert data["status"] == "running"

    def test_get_job_status_not_found(self, client):
        """Return 404 for unknown job."""
        with patch(
            "sql_databricks_bridge.api.routes.jobs._extractors",
            {},
        ):
            response = client.get("/jobs/nonexistent")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_get_job_extractor_exists_job_missing(self, client):
        """Return 404 when extractor exists but job is None."""
        extractor = MagicMock()
        extractor.get_job.return_value = None

        with patch(
            "sql_databricks_bridge.api.routes.jobs._extractors",
            {"test-job": extractor},
        ):
            response = client.get("/jobs/test-job")

        assert response.status_code == 404


class TestCancelJob:
    """Tests for DELETE /jobs/{job_id} endpoint."""

    def test_cancel_running_job(self, client, mock_extractor):
        """Cancel a running job."""
        extractor, job = mock_extractor
        job.status = JobStatus.RUNNING

        with patch(
            "sql_databricks_bridge.api.routes.jobs._extractors",
            {job.job_id: extractor},
        ):
            response = client.delete(f"/jobs/{job.job_id}")

        assert response.status_code == 204
        assert job.status == JobStatus.CANCELLED

    def test_cancel_pending_job(self, client, mock_extractor):
        """Cancel a pending job."""
        extractor, job = mock_extractor
        job.status = JobStatus.PENDING

        with patch(
            "sql_databricks_bridge.api.routes.jobs._extractors",
            {job.job_id: extractor},
        ):
            response = client.delete(f"/jobs/{job.job_id}")

        assert response.status_code == 204
        assert job.status == JobStatus.CANCELLED

    def test_cancel_completed_job_fails(self, client, mock_extractor):
        """Cannot cancel completed job."""
        extractor, job = mock_extractor
        job.status = JobStatus.COMPLETED

        with patch(
            "sql_databricks_bridge.api.routes.jobs._extractors",
            {job.job_id: extractor},
        ):
            response = client.delete(f"/jobs/{job.job_id}")

        assert response.status_code == 400
        assert "Cannot cancel" in response.json()["detail"]

    def test_cancel_failed_job_fails(self, client, mock_extractor):
        """Cannot cancel failed job."""
        extractor, job = mock_extractor
        job.status = JobStatus.FAILED

        with patch(
            "sql_databricks_bridge.api.routes.jobs._extractors",
            {job.job_id: extractor},
        ):
            response = client.delete(f"/jobs/{job.job_id}")

        assert response.status_code == 400

    def test_cancel_nonexistent_job(self, client):
        """Return 404 for unknown job."""
        with patch(
            "sql_databricks_bridge.api.routes.jobs._extractors",
            {},
        ):
            response = client.delete("/jobs/nonexistent")

        assert response.status_code == 404


class TestListJobs:
    """Tests for GET /jobs endpoint."""

    def test_list_jobs_empty(self, client):
        """List jobs when none exist."""
        with patch(
            "sql_databricks_bridge.api.routes.jobs._extractors",
            {},
        ):
            response = client.get("/jobs")

        assert response.status_code == 200
        assert response.json() == []

    def test_list_jobs_with_data(self, client, mock_extractor):
        """List jobs when some exist."""
        extractor, job = mock_extractor

        with patch(
            "sql_databricks_bridge.api.routes.jobs._extractors",
            {job.job_id: extractor},
        ):
            response = client.get("/jobs")

        assert response.status_code == 200
        jobs = response.json()
        assert len(jobs) == 1
        assert jobs[0]["job_id"] == job.job_id

    def test_list_jobs_filter_by_status(self, client):
        """Filter jobs by status."""
        from sql_databricks_bridge.core.extractor import ExtractionJob

        job1 = ExtractionJob(
            job_id="job-1",
            country="CL",
            destination="/vol",
            queries=["q1"],
            chunk_size=1000,
            status=JobStatus.RUNNING,
        )
        job2 = ExtractionJob(
            job_id="job-2",
            country="AR",
            destination="/vol",
            queries=["q1"],
            chunk_size=1000,
            status=JobStatus.COMPLETED,
        )

        extractor1 = MagicMock()
        extractor1.get_job.return_value = job1
        extractor2 = MagicMock()
        extractor2.get_job.return_value = job2

        with patch(
            "sql_databricks_bridge.api.routes.jobs._extractors",
            {"job-1": extractor1, "job-2": extractor2},
        ):
            response = client.get("/jobs", params={"status_filter": "running"})

        assert response.status_code == 200
        jobs = response.json()
        assert len(jobs) == 1
        assert jobs[0]["status"] == "running"

    def test_list_jobs_limit(self, client):
        """Limit number of returned jobs."""
        from sql_databricks_bridge.core.extractor import ExtractionJob

        extractors = {}
        for i in range(5):
            job = ExtractionJob(
                job_id=f"job-{i}",
                country="CL",
                destination="/vol",
                queries=["q1"],
                chunk_size=1000,
            )
            extractor = MagicMock()
            extractor.get_job.return_value = job
            extractors[f"job-{i}"] = extractor

        with patch(
            "sql_databricks_bridge.api.routes.jobs._extractors",
            extractors,
        ):
            response = client.get("/jobs", params={"limit": 2})

        assert response.status_code == 200
        assert len(response.json()) == 2
