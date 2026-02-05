"""Custom exceptions for the SQL-Databricks Bridge SDK."""


class BridgeSDKError(Exception):
    """Base exception for all SDK errors."""

    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} - Details: {self.details}"
        return self.message


class ConnectionError(BridgeSDKError):
    """Raised when connection to the service fails."""

    pass


class AuthenticationError(BridgeSDKError):
    """Raised when authentication fails."""

    pass


class EventNotFoundError(BridgeSDKError):
    """Raised when an event is not found."""

    def __init__(self, event_id: str):
        super().__init__(f"Event not found: {event_id}", {"event_id": event_id})
        self.event_id = event_id


class TimeoutError(BridgeSDKError):
    """Raised when an operation times out."""

    def __init__(self, event_id: str, timeout_seconds: int):
        super().__init__(
            f"Timeout waiting for event {event_id} after {timeout_seconds}s",
            {"event_id": event_id, "timeout_seconds": timeout_seconds},
        )
        self.event_id = event_id
        self.timeout_seconds = timeout_seconds


class ValidationError(BridgeSDKError):
    """Raised when validation fails."""

    pass


class APIError(BridgeSDKError):
    """Raised when the API returns an error."""

    def __init__(self, message: str, status_code: int, response_body: str | None = None):
        super().__init__(
            message,
            {"status_code": status_code, "response_body": response_body},
        )
        self.status_code = status_code
        self.response_body = response_body
