"""Unit tests for the stuck-processing recovery endpoint.

Covers the endpoint logic end-to-end with a mocked Databricks client:
- dry-run never writes
- empty candidate set short-circuits
- quotes in event_id are escaped
- older_than_minutes and limit are forwarded to the SQL
- real run issues the UPDATE and returns the reset ids
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    # No poller needed for this endpoint
    with patch("sql_databricks_bridge.main._event_poller", None):
        from sql_databricks_bridge.main import app
        yield TestClient(app)


@pytest.fixture
def mock_dbx_client():
    """Patch the DatabricksClient factory so we can observe execute_sql calls."""
    fake = MagicMock()
    fake.execute_sql = MagicMock(return_value=[])
    with patch(
        "sql_databricks_bridge.api.routes.sync._build_dbx_client",
        return_value=fake,
    ):
        yield fake


class TestRecoveryDryRun:
    def test_dry_run_returns_candidates_without_updating(
        self, client, mock_dbx_client
    ):
        mock_dbx_client.execute_sql.return_value = [
            {"event_id": "evt-1", "source_table": "s", "target_table": "t",
             "processed_at": "2026-04-20T10:00:00"},
            {"event_id": "evt-2", "source_table": "s", "target_table": "t",
             "processed_at": None},
        ]

        resp = client.post(
            "/api/v1/sync/events/recover-stuck-processing"
            "?older_than_minutes=15&dry_run=true"
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["dry_run"] is True
        assert body["candidates"] == 2
        assert body["reset"] == 0
        assert body["event_ids"] == ["evt-1", "evt-2"]

        # Exactly ONE query issued: the SELECT.  No UPDATE.
        assert mock_dbx_client.execute_sql.call_count == 1
        sql = mock_dbx_client.execute_sql.call_args.args[0]
        assert "SELECT event_id" in sql
        assert "INTERVAL 15 MINUTES" in sql

    def test_empty_candidates_short_circuits(self, client, mock_dbx_client):
        mock_dbx_client.execute_sql.return_value = []

        resp = client.post(
            "/api/v1/sync/events/recover-stuck-processing?dry_run=false"
        )
        body = resp.json()

        assert body["candidates"] == 0
        assert body["reset"] == 0
        assert body["event_ids"] == []
        # No UPDATE issued when no candidates
        assert mock_dbx_client.execute_sql.call_count == 1


class TestRecoveryRealRun:
    def test_reset_updates_and_returns_ids(self, client, mock_dbx_client):
        mock_dbx_client.execute_sql.side_effect = [
            # 1st call: SELECT returns candidates
            [
                {"event_id": "a1", "source_table": "s", "target_table": "t",
                 "processed_at": "2026-04-20T10:00:00"},
                {"event_id": "b2", "source_table": "s", "target_table": "t",
                 "processed_at": "2026-04-20T10:00:00"},
            ],
            # 2nd call: UPDATE returns empty
            [],
        ]

        resp = client.post(
            "/api/v1/sync/events/recover-stuck-processing?dry_run=false"
        )
        body = resp.json()

        assert body["dry_run"] is False
        assert body["candidates"] == 2
        assert body["reset"] == 2
        assert sorted(body["event_ids"]) == ["a1", "b2"]

        assert mock_dbx_client.execute_sql.call_count == 2
        update_sql = mock_dbx_client.execute_sql.call_args_list[1].args[0]
        assert "UPDATE" in update_sql
        assert "'pending'" in update_sql
        assert "'a1'" in update_sql and "'b2'" in update_sql
        # Defensive: must scope only to rows still in 'processing'
        assert "status = 'processing'" in update_sql

    def test_escapes_single_quotes_in_event_id(self, client, mock_dbx_client):
        mock_dbx_client.execute_sql.side_effect = [
            [{"event_id": "weird'id", "source_table": "s",
              "target_table": "t", "processed_at": None}],
            [],
        ]

        resp = client.post(
            "/api/v1/sync/events/recover-stuck-processing?dry_run=false"
        )
        assert resp.status_code == 200

        update_sql = mock_dbx_client.execute_sql.call_args_list[1].args[0]
        # Must contain the escaped form '' rather than the naive single '
        assert "'weird''id'" in update_sql


class TestRecoveryValidation:
    def test_older_than_minutes_lower_bound(self, client, mock_dbx_client):
        resp = client.post(
            "/api/v1/sync/events/recover-stuck-processing"
            "?older_than_minutes=0"
        )
        assert resp.status_code == 422  # pydantic: ge=1

    def test_older_than_minutes_upper_bound(self, client, mock_dbx_client):
        resp = client.post(
            "/api/v1/sync/events/recover-stuck-processing"
            "?older_than_minutes=9999"
        )
        assert resp.status_code == 422  # pydantic: le=1440

    def test_limit_forwarded_to_sql(self, client, mock_dbx_client):
        mock_dbx_client.execute_sql.return_value = []
        resp = client.post(
            "/api/v1/sync/events/recover-stuck-processing?limit=5"
        )
        assert resp.status_code == 200
        sql = mock_dbx_client.execute_sql.call_args.args[0]
        assert "LIMIT 5" in sql

    def test_defaults_are_safe(self, client, mock_dbx_client):
        """Without query args, the endpoint defaults to dry_run=true."""
        mock_dbx_client.execute_sql.return_value = [
            {"event_id": "safe-default", "source_table": "s",
             "target_table": "t", "processed_at": None},
        ]
        resp = client.post("/api/v1/sync/events/recover-stuck-processing")
        body = resp.json()
        assert body["dry_run"] is True
        # Default older_than_minutes=10
        assert "INTERVAL 10 MINUTES" in (
            mock_dbx_client.execute_sql.call_args.args[0]
        )


class TestRecoveryErrors:
    def test_select_failure_returns_500(self, client, mock_dbx_client):
        mock_dbx_client.execute_sql.side_effect = RuntimeError("warehouse down")
        resp = client.post(
            "/api/v1/sync/events/recover-stuck-processing"
        )
        assert resp.status_code == 500
        assert "warehouse down" in resp.json()["detail"]

    def test_update_failure_returns_500(self, client, mock_dbx_client):
        mock_dbx_client.execute_sql.side_effect = [
            [{"event_id": "x", "source_table": "s", "target_table": "t",
              "processed_at": None}],
            RuntimeError("mid-transaction"),
        ]
        resp = client.post(
            "/api/v1/sync/events/recover-stuck-processing?dry_run=false"
        )
        assert resp.status_code == 500
        assert "mid-transaction" in resp.json()["detail"]
