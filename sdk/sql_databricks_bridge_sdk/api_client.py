"""REST API client for the SQL-Databricks Bridge service.

Use this client when interacting with the bridge service via its REST API,
typically from a local development environment.
"""

import time
from typing import Any
from urllib.parse import urljoin

import requests

from sql_databricks_bridge_sdk.exceptions import (
    APIError,
    AuthenticationError,
    ConnectionError,
    EventNotFoundError,
    TimeoutError,
    ValidationError,
)
from sql_databricks_bridge_sdk.models import (
    EventResult,
    SyncEvent,
    SyncEventStatus,
    SyncOperation,
)


class SyncAPI:
    """Sync operations API namespace."""

    def __init__(self, client: "BridgeAPIClient"):
        self._client = client

    def create_event(
        self,
        operation: SyncOperation | str,
        source_table: str,
        target_table: str,
        primary_keys: list[str] | None = None,
        rows_expected: int | None = None,
        filter_conditions: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        priority: int = 5,
        process_immediately: bool = False,
    ) -> SyncEvent:
        """Create a sync event.

        Args:
            operation: Type of operation (INSERT, UPDATE, DELETE)
            source_table: Source table in Databricks (catalog.schema.table)
            target_table: Target table in SQL Server (schema.table)
            primary_keys: Primary key columns for UPDATE/DELETE
            rows_expected: Expected number of rows (for validation)
            filter_conditions: WHERE conditions for DELETE
            metadata: Additional metadata
            priority: Event priority (1-10, lower = higher priority)
            process_immediately: Process synchronously instead of queuing

        Returns:
            Created SyncEvent

        Raises:
            ValidationError: If required parameters are missing
            APIError: If API returns an error
        """
        if isinstance(operation, str):
            operation = SyncOperation(operation)

        # Validate
        if operation in (SyncOperation.UPDATE, SyncOperation.DELETE):
            if not primary_keys:
                raise ValidationError(
                    f"primary_keys required for {operation.value} operation"
                )

        if operation == SyncOperation.DELETE and not filter_conditions:
            raise ValidationError("filter_conditions required for DELETE operation")

        import uuid

        event_id = f"{operation.value.lower()}-{uuid.uuid4().hex[:12]}"

        payload = {
            "event_id": event_id,
            "operation": operation.value,
            "source_table": source_table,
            "target_table": target_table,
            "primary_keys": primary_keys or [],
            "priority": priority,
            "rows_expected": rows_expected,
            "metadata": metadata or {},
        }

        if filter_conditions:
            payload["metadata"]["filter_conditions"] = filter_conditions

        params = {"process_immediately": str(process_immediately).lower()}
        response = self._client._post("/sync/events", json=payload, params=params)

        return SyncEvent.from_dict(response)

    def create_insert_event(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str] | None = None,
        rows_expected: int | None = None,
        metadata: dict[str, Any] | None = None,
        priority: int = 5,
        process_immediately: bool = False,
    ) -> SyncEvent:
        """Create an INSERT sync event.

        Args:
            source_table: Source table in Databricks (catalog.schema.table)
            target_table: Target table in SQL Server (schema.table)
            primary_keys: Primary key columns (for duplicate detection)
            rows_expected: Expected number of rows
            metadata: Additional metadata
            priority: Event priority (1-10)
            process_immediately: Process synchronously

        Returns:
            Created SyncEvent
        """
        return self.create_event(
            operation=SyncOperation.INSERT,
            source_table=source_table,
            target_table=target_table,
            primary_keys=primary_keys,
            rows_expected=rows_expected,
            metadata=metadata,
            priority=priority,
            process_immediately=process_immediately,
        )

    def create_update_event(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        rows_expected: int | None = None,
        metadata: dict[str, Any] | None = None,
        priority: int = 5,
        process_immediately: bool = False,
    ) -> SyncEvent:
        """Create an UPDATE sync event.

        Args:
            source_table: Source table with updated data
            target_table: Target table in SQL Server
            primary_keys: Primary key columns (required)
            rows_expected: Expected number of rows
            metadata: Additional metadata
            priority: Event priority (1-10)
            process_immediately: Process synchronously

        Returns:
            Created SyncEvent
        """
        return self.create_event(
            operation=SyncOperation.UPDATE,
            source_table=source_table,
            target_table=target_table,
            primary_keys=primary_keys,
            rows_expected=rows_expected,
            metadata=metadata,
            priority=priority,
            process_immediately=process_immediately,
        )

    def create_delete_event(
        self,
        target_table: str,
        primary_keys: list[str],
        filter_conditions: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        priority: int = 5,
        process_immediately: bool = False,
    ) -> SyncEvent:
        """Create a DELETE sync event.

        Args:
            target_table: Target table in SQL Server
            primary_keys: Primary key columns (required)
            filter_conditions: WHERE conditions (required)
            metadata: Additional metadata
            priority: Event priority (1-10)
            process_immediately: Process synchronously

        Returns:
            Created SyncEvent
        """
        return self.create_event(
            operation=SyncOperation.DELETE,
            source_table="",
            target_table=target_table,
            primary_keys=primary_keys,
            filter_conditions=filter_conditions,
            metadata=metadata,
            priority=priority,
            process_immediately=process_immediately,
        )

    def get_event(self, event_id: str) -> SyncEvent:
        """Get an event by ID.

        Args:
            event_id: Event ID

        Returns:
            SyncEvent

        Raises:
            EventNotFoundError: If event not found
        """
        try:
            response = self._client._get(f"/sync/events/{event_id}")
            return SyncEvent.from_dict(response)
        except APIError as e:
            if e.status_code == 404:
                raise EventNotFoundError(event_id) from e
            raise

    def list_events(
        self,
        status: SyncEventStatus | str | None = None,
        operation: SyncOperation | str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[SyncEvent]:
        """List sync events.

        Args:
            status: Filter by status
            operation: Filter by operation type
            limit: Maximum number of events to return
            offset: Number of events to skip

        Returns:
            List of SyncEvent objects
        """
        params: dict[str, Any] = {"limit": limit, "offset": offset}

        if status:
            params["status_filter"] = status.value if isinstance(status, SyncEventStatus) else status

        if operation:
            params["operation_filter"] = operation.value if isinstance(operation, SyncOperation) else operation

        response = self._client._get("/sync/events", params=params)
        return [SyncEvent.from_dict(e) for e in response]

    def get_stats(self) -> dict[str, Any]:
        """Get sync statistics.

        Returns:
            Statistics dictionary with total, by_status, by_operation
        """
        return self._client._get("/sync/stats")

    def wait_for_completion(
        self,
        event_id: str,
        timeout_seconds: int = 300,
        poll_interval: int = 5,
    ) -> EventResult:
        """Wait for an event to complete.

        Args:
            event_id: Event ID to wait for
            timeout_seconds: Maximum time to wait
            poll_interval: Seconds between status checks

        Returns:
            EventResult with final status

        Raises:
            TimeoutError: If event doesn't complete in time
            EventNotFoundError: If event not found
        """
        start_time = time.time()

        while True:
            elapsed = time.time() - start_time

            if elapsed >= timeout_seconds:
                raise TimeoutError(event_id, timeout_seconds)

            event = self.get_event(event_id)

            if event.is_completed:
                return EventResult.from_event(event, duration_seconds=elapsed)

            time.sleep(poll_interval)

    def retry_event(self, event_id: str) -> SyncEvent:
        """Retry a failed event.

        Args:
            event_id: Event ID to retry

        Returns:
            Updated SyncEvent
        """
        response = self._client._post(f"/sync/events/{event_id}/retry")
        return SyncEvent.from_dict(response)

    def cancel_event(self, event_id: str) -> None:
        """Cancel a pending event.

        Args:
            event_id: Event ID to cancel
        """
        self._client._delete(f"/sync/events/{event_id}")


class HealthAPI:
    """Health check API namespace."""

    def __init__(self, client: "BridgeAPIClient"):
        self._client = client

    def liveness(self) -> bool:
        """Check if service is alive.

        Returns:
            True if service is responding
        """
        try:
            response = self._client._get("/health/live")
            return response.get("status") == "ok"
        except Exception:
            return False

    def readiness(self) -> dict[str, Any]:
        """Check if service is ready.

        Returns:
            Health status with component details
        """
        return self._client._get("/health/ready")

    def is_ready(self) -> bool:
        """Check if service is fully ready.

        Returns:
            True if all components are healthy
        """
        try:
            response = self.readiness()
            return response.get("status") == "healthy"
        except Exception:
            return False


class BridgeAPIClient:
    """REST API client for the SQL-Databricks Bridge service.

    Use this client for local development to interact with the bridge service
    via its REST API.

    Example:
        ```python
        from sql_databricks_bridge_sdk import BridgeAPIClient

        client = BridgeAPIClient(
            base_url="http://localhost:8000",
            token="my-api-token"
        )

        # Create sync event
        event = client.sync.create_insert_event(
            source_table="catalog.schema.source",
            target_table="dbo.target",
            primary_keys=["id"]
        )

        # Wait for completion
        result = client.sync.wait_for_completion(event.event_id)
        print(f"Rows synced: {result.rows_affected}")
        ```
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        token: str | None = None,
        timeout: int = 30,
        verify_ssl: bool = True,
    ):
        """Initialize the API client.

        Args:
            base_url: Base URL of the bridge service
            token: API token for authentication
            timeout: Request timeout in seconds
            verify_ssl: Whether to verify SSL certificates
        """
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.verify_ssl = verify_ssl

        # API namespaces
        self.sync = SyncAPI(self)
        self.health = HealthAPI(self)

    def _get_headers(self) -> dict[str, str]:
        """Get request headers."""
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> dict[str, Any] | list[Any]:
        """Make HTTP request.

        Args:
            method: HTTP method
            path: API path
            **kwargs: Additional request arguments

        Returns:
            Response JSON

        Raises:
            ConnectionError: If connection fails
            AuthenticationError: If authentication fails
            APIError: If API returns an error
        """
        url = urljoin(self.base_url + "/", path.lstrip("/"))

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self._get_headers(),
                timeout=self.timeout,
                verify=self.verify_ssl,
                **kwargs,
            )
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Failed to connect to {url}") from e
        except requests.exceptions.Timeout as e:
            raise ConnectionError(f"Request to {url} timed out") from e

        if response.status_code == 401:
            raise AuthenticationError("Invalid or missing authentication token")

        if response.status_code == 403:
            raise AuthenticationError("Access denied")

        if response.status_code >= 400:
            try:
                error_body = response.text
            except Exception:
                error_body = None

            raise APIError(
                f"API error: {response.status_code}",
                status_code=response.status_code,
                response_body=error_body,
            )

        if response.status_code == 204:
            return {}

        return response.json()

    def _get(self, path: str, **kwargs: Any) -> Any:
        """Make GET request."""
        return self._request("GET", path, **kwargs)

    def _post(self, path: str, **kwargs: Any) -> Any:
        """Make POST request."""
        return self._request("POST", path, **kwargs)

    def _delete(self, path: str, **kwargs: Any) -> Any:
        """Make DELETE request."""
        return self._request("DELETE", path, **kwargs)

    def test_connection(self) -> bool:
        """Test connection to the service.

        Returns:
            True if connection successful
        """
        return self.health.liveness()
