"""Data models for the SQL-Databricks Bridge SDK."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class SyncOperation(str, Enum):
    """Supported sync operations."""

    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class SyncEventStatus(str, Enum):
    """Event status values."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"


@dataclass
class SyncEvent:
    """Represents a synchronization event."""

    event_id: str
    operation: SyncOperation
    source_table: str
    target_table: str
    primary_keys: list[str] = field(default_factory=list)
    priority: int = 5
    status: SyncEventStatus = SyncEventStatus.PENDING
    rows_expected: int | None = None
    rows_affected: int | None = None
    discrepancy: int | None = None
    warning: str | None = None
    error_message: str | None = None
    filter_conditions: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    created_at: datetime | None = None
    processed_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API/SQL operations."""
        return {
            "event_id": self.event_id,
            "operation": self.operation.value if isinstance(self.operation, SyncOperation) else self.operation,
            "source_table": self.source_table,
            "target_table": self.target_table,
            "primary_keys": self.primary_keys,
            "priority": self.priority,
            "status": self.status.value if isinstance(self.status, SyncEventStatus) else self.status,
            "rows_expected": self.rows_expected,
            "rows_affected": self.rows_affected,
            "discrepancy": self.discrepancy,
            "warning": self.warning,
            "error_message": self.error_message,
            "filter_conditions": self.filter_conditions,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "processed_at": self.processed_at.isoformat() if self.processed_at else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SyncEvent":
        """Create from dictionary (API response or SQL result)."""
        # Parse operation
        operation = data.get("operation", "INSERT")
        if isinstance(operation, str):
            operation = SyncOperation(operation)

        # Parse status
        status = data.get("status", "pending")
        if isinstance(status, str):
            status = SyncEventStatus(status)

        # Parse timestamps
        created_at = data.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))

        processed_at = data.get("processed_at")
        if isinstance(processed_at, str):
            processed_at = datetime.fromisoformat(processed_at.replace("Z", "+00:00"))

        return cls(
            event_id=data["event_id"],
            operation=operation,
            source_table=data.get("source_table", ""),
            target_table=data.get("target_table", ""),
            primary_keys=data.get("primary_keys", []),
            priority=data.get("priority", 5),
            status=status,
            rows_expected=data.get("rows_expected"),
            rows_affected=data.get("rows_affected"),
            discrepancy=data.get("discrepancy"),
            warning=data.get("warning"),
            error_message=data.get("error_message"),
            filter_conditions=data.get("filter_conditions"),
            metadata=data.get("metadata"),
            created_at=created_at,
            processed_at=processed_at,
        )

    @property
    def is_completed(self) -> bool:
        """Check if event is completed (success or failure)."""
        return self.status in (SyncEventStatus.COMPLETED, SyncEventStatus.FAILED)

    @property
    def is_successful(self) -> bool:
        """Check if event completed successfully."""
        return self.status == SyncEventStatus.COMPLETED


@dataclass
class EventResult:
    """Result of waiting for an event to complete."""

    event: SyncEvent
    success: bool
    rows_affected: int
    error: str | None = None
    warning: str | None = None
    duration_seconds: float = 0.0

    @classmethod
    def from_event(cls, event: SyncEvent, duration_seconds: float = 0.0) -> "EventResult":
        """Create result from a completed event."""
        return cls(
            event=event,
            success=event.is_successful,
            rows_affected=event.rows_affected or 0,
            error=event.error_message,
            warning=event.warning,
            duration_seconds=duration_seconds,
        )
