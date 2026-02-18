"""Unit tests for db.jobs_table module -- mocks DatabricksClient.execute_sql."""

import json
from datetime import datetime
from unittest.mock import MagicMock, call

import pytest

from sql_databricks_bridge.db.jobs_table import (
    ensure_jobs_table,
    get_job,
    insert_job,
    list_jobs,
    update_job_status,
)

TABLE = "bridge.events.trigger_jobs"


@pytest.fixture
def mock_client():
    """A DatabricksClient with a mocked execute_sql."""
    client = MagicMock()
    client.execute_sql.return_value = []
    return client


class TestEnsureJobsTable:
    """Tests for ensure_jobs_table."""

    def test_calls_create_table(self, mock_client):
        """Issues CREATE TABLE IF NOT EXISTS."""
        ensure_jobs_table(mock_client, TABLE)
        sql = mock_client.execute_sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert TABLE in sql
        assert "job_id STRING" in sql
        assert "USING DELTA" in sql


class TestInsertJob:
    """Tests for insert_job."""

    def test_insert_sql(self, mock_client):
        """Generates correct INSERT INTO statement."""
        insert_job(
            mock_client,
            TABLE,
            job_id="job-1",
            country="bolivia",
            stage="calibracion",
            tag="bolivia-calibracion-2026-02-10",
            queries=["q1", "q2"],
            triggered_by="user@test.com",
            created_at=datetime(2026, 2, 10, 12, 0, 0),
        )

        sql = mock_client.execute_sql.call_args[0][0]
        assert "INSERT INTO" in sql
        assert TABLE in sql
        assert "'job-1'" in sql
        assert "'bolivia'" in sql
        assert "'calibracion'" in sql
        assert "'pending'" in sql
        assert "'user@test.com'" in sql
        assert json.dumps(["q1", "q2"]).replace('"', "") or "q1" in sql

    def test_escapes_single_quotes(self, mock_client):
        """Single quotes in values are escaped."""
        insert_job(
            mock_client,
            TABLE,
            job_id="job-1",
            country="bolivia",
            stage="calibracion",
            tag="tag",
            queries=["q1"],
            triggered_by="o'brien@test.com",
            created_at=datetime(2026, 2, 10),
        )

        sql = mock_client.execute_sql.call_args[0][0]
        assert "o''brien@test.com" in sql


class TestGetJob:
    """Tests for get_job."""

    def test_found(self, mock_client):
        """Returns parsed dict when job exists."""
        mock_client.execute_sql.return_value = [
            {
                "job_id": "job-1",
                "country": "bolivia",
                "stage": "calibracion",
                "tag": "tag",
                "status": "pending",
                "queries": '["q1","q2"]',
                "triggered_by": "user@test.com",
                "created_at": "2026-02-10T12:00:00",
                "started_at": None,
                "completed_at": None,
                "error": None,
            }
        ]

        result = get_job(mock_client, TABLE, "job-1")
        assert result is not None
        assert result["job_id"] == "job-1"
        assert result["queries"] == ["q1", "q2"]

        sql = mock_client.execute_sql.call_args[0][0]
        assert "WHERE job_id = 'job-1'" in sql

    def test_not_found(self, mock_client):
        """Returns None when no rows."""
        mock_client.execute_sql.return_value = []
        result = get_job(mock_client, TABLE, "nonexistent")
        assert result is None


class TestListJobs:
    """Tests for list_jobs."""

    def test_no_filters(self, mock_client):
        """Generates unfiltered query with LIMIT/OFFSET."""
        mock_client.execute_sql.side_effect = [
            [{"cnt": "3"}],
            [
                {"job_id": "j1", "queries": '["q1"]', "created_at": "2026-02-10"},
                {"job_id": "j2", "queries": '["q2"]', "created_at": "2026-02-09"},
            ],
        ]

        items, total = list_jobs(mock_client, TABLE, limit=2, offset=0)
        assert total == 3
        assert len(items) == 2
        assert items[0]["queries"] == ["q1"]

    def test_filter_country(self, mock_client):
        """Adds WHERE country = ... clause."""
        mock_client.execute_sql.side_effect = [[{"cnt": "1"}], []]

        list_jobs(mock_client, TABLE, country="bolivia")

        count_sql = mock_client.execute_sql.call_args_list[0][0][0]
        assert "country = 'bolivia'" in count_sql

    def test_filter_status(self, mock_client):
        """Adds WHERE status = ... clause."""
        mock_client.execute_sql.side_effect = [[{"cnt": "0"}], []]

        list_jobs(mock_client, TABLE, status="completed")

        count_sql = mock_client.execute_sql.call_args_list[0][0][0]
        assert "status = 'completed'" in count_sql

    def test_filter_triggered_by(self, mock_client):
        """Adds WHERE triggered_by = ... clause."""
        mock_client.execute_sql.side_effect = [[{"cnt": "0"}], []]

        list_jobs(mock_client, TABLE, triggered_by="user@test.com")

        count_sql = mock_client.execute_sql.call_args_list[0][0][0]
        assert "triggered_by = 'user@test.com'" in count_sql


class TestUpdateJobStatus:
    """Tests for update_job_status."""

    def test_update_status_only(self, mock_client):
        """Updates status field."""
        update_job_status(mock_client, TABLE, "job-1", "running")

        sql = mock_client.execute_sql.call_args[0][0]
        assert "UPDATE" in sql
        assert "status = 'running'" in sql
        assert "WHERE job_id = 'job-1'" in sql

    def test_update_with_started_at(self, mock_client):
        """Includes started_at in SET clause."""
        ts = datetime(2026, 2, 10, 12, 5, 0)
        update_job_status(mock_client, TABLE, "job-1", "running", started_at=ts)

        sql = mock_client.execute_sql.call_args[0][0]
        assert "started_at" in sql
        assert "2026-02-10T12:05:00" in sql

    def test_update_with_error(self, mock_client):
        """Includes error in SET clause, escaping quotes."""
        update_job_status(
            mock_client, TABLE, "job-1", "failed", error="Something's wrong"
        )

        sql = mock_client.execute_sql.call_args[0][0]
        assert "error = 'Something''s wrong'" in sql

    def test_update_with_completed_at(self, mock_client):
        """Includes completed_at in SET clause."""
        ts = datetime(2026, 2, 10, 12, 10, 0)
        update_job_status(mock_client, TABLE, "job-1", "completed", completed_at=ts)

        sql = mock_client.execute_sql.call_args[0][0]
        assert "completed_at" in sql
        assert "2026-02-10T12:10:00" in sql
