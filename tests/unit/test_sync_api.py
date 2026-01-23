"""Unit tests for sync API endpoints."""

import uuid
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from sql_databricks_bridge.api.schemas import SyncEventOperation, SyncEventStatus


@pytest.fixture
def mock_sync_operator():
    """Create a mock SyncOperator."""
    mock = MagicMock()
    mock.process_event = AsyncMock()
    return mock


@pytest.fixture
def client():
    """Create test client with mocked dependencies."""
    # Patch the poller to avoid startup issues
    with patch("sql_databricks_bridge.main._event_poller", None):
        from sql_databricks_bridge.main import app

        yield TestClient(app)


@pytest.fixture
def sample_event():
    """Create a sample sync event."""
    return {
        "event_id": str(uuid.uuid4()),
        "operation": "INSERT",
        "source_table": "catalog.schema.source",
        "target_table": "dbo.target",
        "primary_keys": ["id"],
        "priority": 1,
    }


class TestSubmitSyncEvent:
    """Tests for POST /sync/events."""

    def test_submit_event_success(self, client, sample_event):
        """Successfully submit a sync event."""
        response = client.post("/sync/events", json=sample_event)

        assert response.status_code == 202
        data = response.json()
        assert data["event_id"] == sample_event["event_id"]
        assert data["operation"] == "INSERT"
        assert data["status"] == "pending"

    def test_submit_event_process_immediately(self, sample_event):
        """Submit and process event immediately."""
        from sql_databricks_bridge.api.routes.sync import get_sync_operator
        from sql_databricks_bridge.main import app
        from sql_databricks_bridge.sync.operations import OperationResult

        # Create mock operator
        mock_operator = MagicMock()
        mock_operator.process_event = AsyncMock(
            return_value=OperationResult(success=True, rows_affected=100)
        )

        # Override dependency
        app.dependency_overrides[get_sync_operator] = lambda: mock_operator

        try:
            with patch("sql_databricks_bridge.main._event_poller", None):
                client = TestClient(app)
                response = client.post(
                    "/sync/events",
                    json=sample_event,
                    params={"process_immediately": True},
                )

            assert response.status_code == 202
            data = response.json()
            assert data["status"] == "completed"
            assert data["rows_affected"] == 100
        finally:
            app.dependency_overrides.clear()

    def test_submit_update_without_pks_fails(self, client):
        """UPDATE without primary keys should fail."""
        event = {
            "event_id": str(uuid.uuid4()),
            "operation": "UPDATE",
            "source_table": "catalog.schema.source",
            "target_table": "dbo.target",
            "primary_keys": [],  # Empty PKs
        }

        response = client.post("/sync/events", json=event)

        assert response.status_code == 400
        assert "Primary keys required" in response.json()["detail"]

    def test_submit_delete_without_pks_fails(self, client):
        """DELETE without primary keys should fail."""
        event = {
            "event_id": str(uuid.uuid4()),
            "operation": "DELETE",
            "source_table": "catalog.schema.source",
            "target_table": "dbo.target",
            "primary_keys": [],
        }

        response = client.post("/sync/events", json=event)

        assert response.status_code == 400
        assert "Primary keys required" in response.json()["detail"]

    def test_submit_insert_without_pks_succeeds(self, client):
        """INSERT without primary keys should succeed."""
        event = {
            "event_id": str(uuid.uuid4()),
            "operation": "INSERT",
            "source_table": "catalog.schema.source",
            "target_table": "dbo.target",
            "primary_keys": [],  # PKs optional for INSERT
        }

        response = client.post("/sync/events", json=event)

        assert response.status_code == 202


class TestListSyncEvents:
    """Tests for GET /sync/events."""

    def test_list_events_empty(self, client):
        """List events when none exist."""
        # Clear any existing events
        from sql_databricks_bridge.api.routes.sync import _sync_events

        _sync_events.clear()

        response = client.get("/sync/events")

        assert response.status_code == 200
        assert response.json() == []

    def test_list_events_with_data(self, client, sample_event):
        """List events after submitting some."""
        # Submit events
        client.post("/sync/events", json=sample_event)

        event2 = sample_event.copy()
        event2["event_id"] = str(uuid.uuid4())
        event2["operation"] = "UPDATE"
        client.post("/sync/events", json=event2)

        response = client.get("/sync/events")

        assert response.status_code == 200
        events = response.json()
        assert len(events) >= 2

    def test_list_events_filter_by_status(self, client, sample_event):
        """Filter events by status."""
        from sql_databricks_bridge.api.routes.sync import _sync_events

        _sync_events.clear()

        # Submit event
        client.post("/sync/events", json=sample_event)

        # Filter by pending
        response = client.get("/sync/events", params={"status_filter": "pending"})
        assert response.status_code == 200
        events = response.json()
        assert all(e["status"] == "pending" for e in events)

        # Filter by completed (should be empty)
        response = client.get("/sync/events", params={"status_filter": "completed"})
        assert response.status_code == 200
        assert len(response.json()) == 0

    def test_list_events_filter_by_operation(self, client, sample_event):
        """Filter events by operation type."""
        from sql_databricks_bridge.api.routes.sync import _sync_events

        _sync_events.clear()

        client.post("/sync/events", json=sample_event)

        response = client.get("/sync/events", params={"operation_filter": "INSERT"})
        assert response.status_code == 200
        events = response.json()
        assert len(events) == 1

        response = client.get("/sync/events", params={"operation_filter": "DELETE"})
        assert response.status_code == 200
        assert len(response.json()) == 0

    def test_list_events_pagination(self, client):
        """Test pagination with limit and offset."""
        from sql_databricks_bridge.api.routes.sync import _sync_events

        _sync_events.clear()

        # Submit 5 events
        for i in range(5):
            event = {
                "event_id": f"evt-{i}",
                "operation": "INSERT",
                "source_table": "catalog.schema.source",
                "target_table": "dbo.target",
            }
            client.post("/sync/events", json=event)

        # Get first 2
        response = client.get("/sync/events", params={"limit": 2, "offset": 0})
        assert len(response.json()) == 2

        # Get next 2
        response = client.get("/sync/events", params={"limit": 2, "offset": 2})
        assert len(response.json()) == 2


class TestGetSyncEvent:
    """Tests for GET /sync/events/{event_id}."""

    def test_get_event_success(self, client, sample_event):
        """Get an existing event."""
        client.post("/sync/events", json=sample_event)

        response = client.get(f"/sync/events/{sample_event['event_id']}")

        assert response.status_code == 200
        data = response.json()
        assert data["event_id"] == sample_event["event_id"]

    def test_get_event_not_found(self, client):
        """Get a non-existent event."""
        response = client.get("/sync/events/nonexistent-id")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()


class TestRetrySyncEvent:
    """Tests for POST /sync/events/{event_id}/retry."""

    def test_retry_failed_event(self, sample_event):
        """Retry a failed event."""
        from sql_databricks_bridge.api.routes.sync import _sync_events, get_sync_operator
        from sql_databricks_bridge.api.schemas import SyncEvent, SyncEventStatus
        from sql_databricks_bridge.main import app
        from sql_databricks_bridge.sync.operations import OperationResult

        # Create a failed event
        event = SyncEvent(
            event_id=sample_event["event_id"],
            operation=SyncEventOperation.INSERT,
            source_table=sample_event["source_table"],
            target_table=sample_event["target_table"],
            status=SyncEventStatus.FAILED,
            error_message="Previous failure",
        )
        _sync_events[event.event_id] = event

        # Create mock operator
        mock_operator = MagicMock()
        mock_operator.process_event = AsyncMock(
            return_value=OperationResult(success=True, rows_affected=50)
        )

        # Override dependency
        app.dependency_overrides[get_sync_operator] = lambda: mock_operator

        try:
            with patch("sql_databricks_bridge.main._event_poller", None):
                client = TestClient(app)
                response = client.post(f"/sync/events/{event.event_id}/retry")

            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "completed"
            assert data["rows_affected"] == 50
            assert data["error_message"] is None
        finally:
            app.dependency_overrides.clear()

    def test_retry_pending_event_fails(self, client, sample_event):
        """Cannot retry a pending event."""
        client.post("/sync/events", json=sample_event)

        response = client.post(f"/sync/events/{sample_event['event_id']}/retry")

        assert response.status_code == 400
        assert "Cannot retry" in response.json()["detail"]

    def test_retry_nonexistent_event(self, client):
        """Retry a non-existent event."""
        response = client.post("/sync/events/nonexistent/retry")

        assert response.status_code == 404


class TestCancelSyncEvent:
    """Tests for DELETE /sync/events/{event_id}."""

    def test_cancel_pending_event(self, client, sample_event):
        """Cancel a pending event."""
        client.post("/sync/events", json=sample_event)

        response = client.delete(f"/sync/events/{sample_event['event_id']}")

        assert response.status_code == 204

        # Verify it's gone
        response = client.get(f"/sync/events/{sample_event['event_id']}")
        assert response.status_code == 404

    def test_cancel_completed_event_fails(self, sample_event):
        """Cannot cancel a completed event."""
        from sql_databricks_bridge.api.routes.sync import get_sync_operator
        from sql_databricks_bridge.main import app
        from sql_databricks_bridge.sync.operations import OperationResult

        # Create mock operator
        mock_operator = MagicMock()
        mock_operator.process_event = AsyncMock(
            return_value=OperationResult(success=True, rows_affected=10)
        )

        # Override dependency
        app.dependency_overrides[get_sync_operator] = lambda: mock_operator

        try:
            with patch("sql_databricks_bridge.main._event_poller", None):
                client = TestClient(app)

                # Submit and process
                client.post(
                    "/sync/events",
                    json=sample_event,
                    params={"process_immediately": True},
                )

                response = client.delete(f"/sync/events/{sample_event['event_id']}")

            assert response.status_code == 400
            assert "Cannot cancel" in response.json()["detail"]
        finally:
            app.dependency_overrides.clear()

    def test_cancel_nonexistent_event(self, client):
        """Cancel a non-existent event."""
        response = client.delete("/sync/events/nonexistent")

        assert response.status_code == 404


class TestSyncStats:
    """Tests for GET /sync/stats."""

    def test_get_stats_empty(self, client):
        """Get stats when no events exist."""
        from sql_databricks_bridge.api.routes.sync import _sync_events

        _sync_events.clear()

        response = client.get("/sync/stats")

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0

    def test_get_stats_with_events(self, client, sample_event):
        """Get stats after submitting events."""
        from sql_databricks_bridge.api.routes.sync import _sync_events

        _sync_events.clear()

        # Submit multiple events
        client.post("/sync/events", json=sample_event)

        event2 = sample_event.copy()
        event2["event_id"] = str(uuid.uuid4())
        event2["operation"] = "UPDATE"
        event2["primary_keys"] = ["id"]
        client.post("/sync/events", json=event2)

        response = client.get("/sync/stats")

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
        assert "pending" in data["by_status"]
        assert data["by_status"]["pending"] == 2


class TestPollerStatus:
    """Tests for poller status endpoints."""

    def test_get_poller_status_disabled(self, client):
        """Get poller status when disabled."""
        response = client.get("/sync/poller/status")

        assert response.status_code == 200
        data = response.json()
        assert data["enabled"] is False

    def test_trigger_poll_when_disabled(self, client):
        """Trigger poll when poller is disabled."""
        response = client.post("/sync/poller/trigger")

        assert response.status_code == 503
        assert "not configured" in response.json()["detail"]
