"""Unit tests for db.local_store module -- uses a real in-memory SQLite DB via tmp_path."""

import pytest

from sql_databricks_bridge.db import local_store


@pytest.fixture
def db(tmp_path):
    """Initialize a fresh SQLite database in a temp directory."""
    db_path = str(tmp_path / "jobs.db")
    local_store.init_db(db_path)
    return db_path


def _insert_completed(db, *, job_id, country, completed_at):
    """Helper: insert a job and immediately mark it completed."""
    local_store.insert_job(
        db,
        job_id=job_id,
        country=country,
        stage="calibracion",
        tag=f"{country}-calibracion",
        queries=["q1"],
        triggered_by="test@test.com",
        created_at="2026-01-01T00:00:00",
    )
    local_store.update_job(db, job_id, status="completed", completed_at=completed_at)


class TestInitDb:
    """Tests for init_db."""

    def test_creates_database_file(self, tmp_path):
        """init_db creates the SQLite file and returns its path."""
        db_path = str(tmp_path / "test.db")
        result = local_store.init_db(db_path)
        assert result == db_path
        assert (tmp_path / "test.db").exists()

    def test_idempotent(self, tmp_path):
        """Calling init_db twice on the same path does not raise."""
        db_path = str(tmp_path / "test.db")
        local_store.init_db(db_path)
        local_store.init_db(db_path)  # must not raise


class TestInsertAndGetJob:
    """Tests for insert_job + get_job round-trip."""

    def test_get_inserted_job(self, db):
        """get_job returns the job that was just inserted."""
        local_store.insert_job(
            db,
            job_id="job-1",
            country="bolivia",
            stage="calibracion",
            tag="bolivia-calibracion",
            queries=["q1", "q2"],
            triggered_by="user@test.com",
            created_at="2026-02-10T12:00:00",
        )
        job = local_store.get_job(db, "job-1")
        assert job is not None
        assert job["job_id"] == "job-1"
        assert job["country"] == "bolivia"
        assert job["status"] == "pending"
        assert job["queries"] == ["q1", "q2"]

    def test_get_missing_job_returns_none(self, db):
        """get_job returns None for an unknown job_id."""
        assert local_store.get_job(db, "nonexistent") is None


class TestGetLastCompletedPerCountry:
    """Tests for get_last_completed_per_country."""

    def test_empty_database_returns_empty_list(self, db):
        """Returns an empty list when no completed jobs exist."""
        result = local_store.get_last_completed_per_country(db)
        assert result == []

    def test_no_completed_jobs_returns_empty_list(self, db):
        """Returns empty list when jobs exist but none are completed."""
        local_store.insert_job(
            db,
            job_id="job-pending",
            country="bolivia",
            stage="calibracion",
            tag="bolivia-calibracion",
            queries=["q1"],
            triggered_by="user@test.com",
            created_at="2026-02-10T12:00:00",
        )
        result = local_store.get_last_completed_per_country(db)
        assert result == []

    def test_single_completed_job(self, db):
        """Returns that job when exactly one completed job exists."""
        _insert_completed(db, job_id="job-1", country="bolivia", completed_at="2026-02-10T12:00:00")

        result = local_store.get_last_completed_per_country(db)
        assert len(result) == 1
        assert result[0]["country"] == "bolivia"
        assert result[0]["job_id"] == "job-1"
        assert result[0]["completed_at"] == "2026-02-10T12:00:00"

    def test_returns_latest_completed_per_country(self, db):
        """Returns the most recent completed job for each country, not older ones."""
        # Bolivia: two completed jobs; the later one should win
        _insert_completed(
            db, job_id="bolivia-old", country="bolivia", completed_at="2026-02-01T10:00:00"
        )
        _insert_completed(
            db, job_id="bolivia-new", country="bolivia", completed_at="2026-02-10T15:00:00"
        )

        result = local_store.get_last_completed_per_country(db)
        assert len(result) == 1
        assert result[0]["country"] == "bolivia"
        assert result[0]["job_id"] == "bolivia-new"

    def test_returns_one_row_per_country(self, db):
        """Each country appears exactly once in the result."""
        _insert_completed(
            db, job_id="bolivia-1", country="bolivia", completed_at="2026-02-10T10:00:00"
        )
        _insert_completed(
            db, job_id="colombia-1", country="colombia", completed_at="2026-02-09T10:00:00"
        )
        _insert_completed(
            db, job_id="peru-1", country="peru", completed_at="2026-02-08T10:00:00"
        )

        result = local_store.get_last_completed_per_country(db)
        countries = {r["country"] for r in result}
        assert countries == {"bolivia", "colombia", "peru"}
        assert len(result) == 3

    def test_ignores_non_completed_statuses(self, db):
        """Jobs with status pending, running, or failed are excluded."""
        local_store.insert_job(
            db,
            job_id="job-pending",
            country="bolivia",
            stage="calibracion",
            tag="tag",
            queries=["q1"],
            triggered_by="u@test.com",
            created_at="2026-02-10T12:00:00",
        )
        local_store.insert_job(
            db,
            job_id="job-failed",
            country="colombia",
            stage="calibracion",
            tag="tag",
            queries=["q1"],
            triggered_by="u@test.com",
            created_at="2026-02-10T12:00:00",
        )
        local_store.update_job(
            db, "job-failed", status="failed", completed_at="2026-02-10T13:00:00"
        )

        result = local_store.get_last_completed_per_country(db)
        assert result == []

    def test_result_includes_required_fields(self, db):
        """Each result dict has country, job_id, stage, and completed_at."""
        _insert_completed(
            db, job_id="job-1", country="bolivia", completed_at="2026-02-10T12:00:00"
        )

        result = local_store.get_last_completed_per_country(db)
        assert len(result) == 1
        row = result[0]
        assert "country" in row
        assert "job_id" in row
        assert "stage" in row
        assert "completed_at" in row

    def test_mixed_completed_and_non_completed(self, db):
        """Only the completed jobs appear; non-completed jobs for the same country are skipped."""
        _insert_completed(
            db, job_id="bolivia-done", country="bolivia", completed_at="2026-02-10T10:00:00"
        )
        # A pending job for the same country that has a later created_at
        local_store.insert_job(
            db,
            job_id="bolivia-pending",
            country="bolivia",
            stage="calibracion",
            tag="tag",
            queries=["q1"],
            triggered_by="u@test.com",
            created_at="2026-02-11T12:00:00",
        )

        result = local_store.get_last_completed_per_country(db)
        assert len(result) == 1
        assert result[0]["job_id"] == "bolivia-done"
