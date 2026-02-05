"""Unit tests for extractor module."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from sql_databricks_bridge.api.schemas import JobStatus
from sql_databricks_bridge.core.extractor import ExtractionJob, Extractor


@pytest.fixture
def sample_queries_dir():
    """Create temp directory with country-aware query structure."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create directory structure: queries/countries/{country}/
        base_path = Path(tmpdir)

        # Create common queries directory (can be empty for now)
        common_dir = base_path / "common"
        common_dir.mkdir(parents=True)

        # Create Chile-specific queries
        cl_dir = base_path / "countries" / "CL"
        cl_dir.mkdir(parents=True)

        query1 = cl_dir / "query1.sql"
        query1.write_text("SELECT * FROM dbo.users WHERE country = 'CL'")

        query2 = cl_dir / "query2.sql"
        query2.write_text("SELECT id, name FROM dbo.orders")

        # Create Bolivia-specific queries
        bo_dir = base_path / "countries" / "BO"
        bo_dir.mkdir(parents=True)

        query1_bo = bo_dir / "query1.sql"
        query1_bo.write_text("SELECT * FROM dbo.users WHERE country = 'BO'")

        yield tmpdir


@pytest.fixture
def mock_sql_client():
    """Create mock SQL client."""
    client = MagicMock()
    return client


class TestExtractionJob:
    """Tests for ExtractionJob dataclass."""

    def test_job_default_status(self):
        """New job has pending status."""
        job = ExtractionJob(
            job_id="test-1",
            country="CL",
            destination="/volumes/test",
            queries=["query1"],
            chunk_size=1000,
        )
        assert job.status == JobStatus.PENDING

    def test_job_queries_completed(self):
        """Count completed queries."""
        from sql_databricks_bridge.api.schemas import QueryResult

        job = ExtractionJob(
            job_id="test-1",
            country="CL",
            destination="/volumes/test",
            queries=["q1", "q2", "q3"],
            chunk_size=1000,
        )
        job.results = [
            QueryResult(query_name="q1", status=JobStatus.COMPLETED),
            QueryResult(query_name="q2", status=JobStatus.COMPLETED),
            QueryResult(query_name="q3", status=JobStatus.FAILED),
        ]

        assert job.queries_completed == 2
        assert job.queries_failed == 1


class TestExtractor:
    """Tests for Extractor class."""

    def test_extractor_init(self, sample_queries_dir, mock_sql_client):
        """Initialize extractor with paths."""
        extractor = Extractor(
            queries_path=sample_queries_dir,
            sql_client=mock_sql_client,
        )

        assert extractor.sql_client is mock_sql_client
        assert extractor.query_loader is not None

    def test_create_job(self, sample_queries_dir, mock_sql_client):
        """Create extraction job."""
        extractor = Extractor(
            queries_path=sample_queries_dir,
            sql_client=mock_sql_client,
        )

        job = extractor.create_job(
            country="CL",
            destination="/volumes/test",
            queries=["query1"],
            chunk_size=5000,
        )

        assert job.job_id is not None
        assert job.country == "CL"
        assert job.destination == "/volumes/test"
        assert job.queries == ["query1"]
        assert job.chunk_size == 5000

    def test_create_job_all_queries(self, sample_queries_dir, mock_sql_client):
        """Create job with all available queries."""
        extractor = Extractor(
            queries_path=sample_queries_dir,
            sql_client=mock_sql_client,
        )

        job = extractor.create_job(
            country="CL",
            destination="/volumes/test",
            # No queries specified = all
        )

        assert len(job.queries) == 2
        assert "query1" in job.queries
        assert "query2" in job.queries

    def test_create_job_invalid_query(self, sample_queries_dir, mock_sql_client):
        """Raise error for unknown queries."""
        extractor = Extractor(
            queries_path=sample_queries_dir,
            sql_client=mock_sql_client,
        )

        with pytest.raises(ValueError) as exc_info:
            extractor.create_job(
                country="CL",
                destination="/volumes/test",
                queries=["nonexistent_query"],
            )

        assert "not available for" in str(exc_info.value)

    def test_get_job(self, sample_queries_dir, mock_sql_client):
        """Get job by ID."""
        extractor = Extractor(
            queries_path=sample_queries_dir,
            sql_client=mock_sql_client,
        )

        job = extractor.create_job(
            country="CL",
            destination="/volumes/test",
        )

        retrieved = extractor.get_job(job.job_id)
        assert retrieved is job

    def test_get_job_not_found(self, sample_queries_dir, mock_sql_client):
        """Return None for unknown job ID."""
        extractor = Extractor(
            queries_path=sample_queries_dir,
            sql_client=mock_sql_client,
        )

        assert extractor.get_job("nonexistent") is None

    def test_execute_query(self, sample_queries_dir, mock_sql_client):
        """Execute single query and get chunks."""
        # Setup mock to return chunks
        chunk1 = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        chunk2 = pl.DataFrame({"id": [3, 4], "name": ["c", "d"]})
        mock_sql_client.execute_query_chunked.return_value = iter([chunk1, chunk2])

        extractor = Extractor(
            queries_path=sample_queries_dir,
            sql_client=mock_sql_client,
        )

        chunks = list(
            extractor.execute_query(
                query_name="query1",
                country="CL",
                chunk_size=2,
            )
        )

        assert len(chunks) == 2
        assert len(chunks[0]) == 2
        assert len(chunks[1]) == 2

    def test_run_extraction_success(self, sample_queries_dir, mock_sql_client):
        """Run extraction job successfully."""
        # Setup mock to return data
        chunk = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        mock_sql_client.execute_query_chunked.return_value = iter([chunk])

        extractor = Extractor(
            queries_path=sample_queries_dir,
            sql_client=mock_sql_client,
        )

        job = extractor.create_job(
            country="CL",
            destination="/volumes/test",
            queries=["query1"],
        )

        result = extractor.run_extraction(job)

        assert result.status == JobStatus.COMPLETED
        assert result.started_at is not None
        assert result.completed_at is not None
        assert len(result.results) == 1
        assert result.results[0].rows_extracted == 3

    def test_run_extraction_empty_result(self, sample_queries_dir, mock_sql_client):
        """Handle empty query results."""
        mock_sql_client.execute_query_chunked.return_value = iter([])

        extractor = Extractor(
            queries_path=sample_queries_dir,
            sql_client=mock_sql_client,
        )

        job = extractor.create_job(
            country="CL",
            destination="/volumes/test",
            queries=["query1"],
        )

        result = extractor.run_extraction(job)

        assert result.status == JobStatus.COMPLETED
        assert result.results[0].rows_extracted == 0

    def test_run_extraction_with_error(self, sample_queries_dir, mock_sql_client):
        """Handle query execution errors."""
        mock_sql_client.execute_query_chunked.side_effect = Exception("DB error")

        extractor = Extractor(
            queries_path=sample_queries_dir,
            sql_client=mock_sql_client,
        )

        job = extractor.create_job(
            country="CL",
            destination="/volumes/test",
            queries=["query1"],
        )

        result = extractor.run_extraction(job)

        assert result.status == JobStatus.FAILED
        assert result.results[0].status == JobStatus.FAILED
        assert "DB error" in result.results[0].error

    def test_run_extraction_partial_success(self, sample_queries_dir, mock_sql_client):
        """Handle partial success (some queries fail)."""
        # First query succeeds, second fails
        chunk = pl.DataFrame({"id": [1], "name": ["a"]})

        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return iter([chunk])  # First call succeeds
            raise Exception("Error")  # Second call fails

        mock_sql_client.execute_query_chunked.side_effect = side_effect

        extractor = Extractor(
            queries_path=sample_queries_dir,
            sql_client=mock_sql_client,
        )

        job = extractor.create_job(
            country="CL",
            destination="/volumes/test",
            queries=["query1", "query2"],
        )

        result = extractor.run_extraction(job)

        # Partial success: some completed, some failed -> status COMPLETED
        # Based on the logic in extractor.py:
        # if queries_failed > 0 and queries_completed == 0 -> FAILED
        # else -> COMPLETED
        assert result.queries_completed == 1
        assert result.queries_failed == 1
        assert result.status == JobStatus.COMPLETED
