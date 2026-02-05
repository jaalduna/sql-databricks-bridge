"""SQL-Databricks Bridge SDK.

A Python SDK for interacting with the SQL-Databricks Bridge service.

Provides two main clients:
- BridgeAPIClient: For interacting with the REST API (local development)
- BridgeEventsClient: For interacting directly with Databricks events table (jobs/notebooks)
"""

from sql_databricks_bridge_sdk.api_client import BridgeAPIClient
from sql_databricks_bridge_sdk.events_client import BridgeEventsClient
from sql_databricks_bridge_sdk.models import (
    SyncEvent,
    SyncEventStatus,
    SyncOperation,
    EventResult,
)
from sql_databricks_bridge_sdk.exceptions import (
    BridgeSDKError,
    ConnectionError,
    AuthenticationError,
    EventNotFoundError,
    TimeoutError,
    ValidationError,
)

__version__ = "0.1.0"
__all__ = [
    # Clients
    "BridgeAPIClient",
    "BridgeEventsClient",
    # Models
    "SyncEvent",
    "SyncEventStatus",
    "SyncOperation",
    "EventResult",
    # Exceptions
    "BridgeSDKError",
    "ConnectionError",
    "AuthenticationError",
    "EventNotFoundError",
    "TimeoutError",
    "ValidationError",
]
