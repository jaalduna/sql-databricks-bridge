"""Event models for sync operations."""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class SyncOperation(str, Enum):
    """Sync operation types."""

    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class SyncStatus(str, Enum):
    """Sync event status."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"
    RETRY = "retry"


class SyncEvent(BaseModel):
    """Event for Databricks → SQL sync.

    This model represents a row in the bridge_events table.
    """

    event_id: str = Field(..., description="Unique event identifier")
    operation: SyncOperation = Field(..., description="Operation type")
    source_table: str = Field(
        ...,
        description="Databricks source table (catalog.schema.table)",
    )
    target_table: str = Field(
        ...,
        description="SQL Server target table (schema.table)",
    )
    primary_keys: list[str] = Field(
        default_factory=list,
        description="Primary key columns for UPDATE/DELETE",
    )
    priority: int = Field(default=0, description="Higher = processed first")
    status: SyncStatus = Field(default=SyncStatus.PENDING)

    # Result tracking
    rows_expected: int | None = Field(default=None, description="Expected row count")
    rows_affected: int | None = Field(default=None, description="Actual rows affected")
    discrepancy: int | None = Field(default=None, description="Expected - Affected")
    warning: str | None = Field(default=None, description="Warning message")
    error_message: str | None = Field(default=None, description="Error details")

    # Retry tracking
    retry_count: int = Field(default=0)
    max_retries: int = Field(default=3)

    # Timestamps
    created_at: datetime | None = None
    processed_at: datetime | None = None

    # Additional data
    filter_conditions: dict[str, Any] = Field(
        default_factory=dict,
        description="WHERE clause conditions for UPDATE/DELETE",
    )
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def can_retry(self) -> bool:
        """Check if event can be retried."""
        return self.retry_count < self.max_retries

    @property
    def has_discrepancy(self) -> bool:
        """Check if there's a discrepancy between expected and actual."""
        if self.rows_expected is None or self.rows_affected is None:
            return False
        return self.rows_expected != self.rows_affected


class SyncBatch(BaseModel):
    """Batch of sync events."""

    batch_id: str
    events: list[SyncEvent]
    created_at: datetime = Field(default_factory=datetime.utcnow)

    @property
    def total_events(self) -> int:
        return len(self.events)

    @property
    def pending_count(self) -> int:
        return sum(1 for e in self.events if e.status == SyncStatus.PENDING)

    @property
    def completed_count(self) -> int:
        return sum(1 for e in self.events if e.status == SyncStatus.COMPLETED)

    @property
    def failed_count(self) -> int:
        return sum(1 for e in self.events if e.status == SyncStatus.FAILED)
