"""Audit logging for security-relevant events."""

import json
import logging
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel

logger = logging.getLogger("sql_databricks_bridge.audit")


class AuditEventType(str, Enum):
    """Types of audit events."""

    AUTH_SUCCESS = "auth_success"
    AUTH_FAILURE = "auth_failure"
    PERMISSION_GRANTED = "permission_granted"
    PERMISSION_DENIED = "permission_denied"
    EXTRACTION_STARTED = "extraction_started"
    EXTRACTION_COMPLETED = "extraction_completed"
    EXTRACTION_FAILED = "extraction_failed"
    SYNC_STARTED = "sync_started"
    SYNC_COMPLETED = "sync_completed"
    SYNC_FAILED = "sync_failed"
    DELETE_BLOCKED = "delete_blocked"


class AuditEvent(BaseModel):
    """Audit event record."""

    timestamp: datetime
    event_type: AuditEventType
    user_name: str | None = None
    user_token_prefix: str | None = None
    source_ip: str | None = None
    resource: str | None = None
    action: str | None = None
    status: str | None = None
    details: dict[str, Any] | None = None
    error: str | None = None


class AuditLogger:
    """Logs audit events for security tracking."""

    def __init__(self, log_to_file: str | None = None) -> None:
        """Initialize audit logger.

        Args:
            log_to_file: Optional file path for JSON audit log.
        """
        self.log_to_file = log_to_file

        if log_to_file:
            file_handler = logging.FileHandler(log_to_file)
            file_handler.setFormatter(logging.Formatter("%(message)s"))
            logger.addHandler(file_handler)

    def _get_token_prefix(self, token: str | None) -> str | None:
        """Get first 8 chars of token for logging."""
        if not token:
            return None
        return token[:8] + "..."

    def log(self, event: AuditEvent) -> None:
        """Log an audit event.

        Args:
            event: Audit event to log.
        """
        log_data = event.model_dump(mode="json", exclude_none=True)
        log_message = json.dumps(log_data)

        # Log at appropriate level
        if event.event_type in (
            AuditEventType.AUTH_FAILURE,
            AuditEventType.PERMISSION_DENIED,
            AuditEventType.DELETE_BLOCKED,
        ):
            logger.warning(log_message)
        elif "failed" in event.event_type.value:
            logger.error(log_message)
        else:
            logger.info(log_message)

    def log_auth_success(
        self,
        user_name: str,
        token: str,
        source_ip: str | None = None,
    ) -> None:
        """Log successful authentication."""
        self.log(
            AuditEvent(
                timestamp=datetime.utcnow(),
                event_type=AuditEventType.AUTH_SUCCESS,
                user_name=user_name,
                user_token_prefix=self._get_token_prefix(token),
                source_ip=source_ip,
                status="success",
            )
        )

    def log_auth_failure(
        self,
        token: str | None = None,
        source_ip: str | None = None,
        reason: str | None = None,
    ) -> None:
        """Log failed authentication."""
        self.log(
            AuditEvent(
                timestamp=datetime.utcnow(),
                event_type=AuditEventType.AUTH_FAILURE,
                user_token_prefix=self._get_token_prefix(token),
                source_ip=source_ip,
                status="failure",
                error=reason,
            )
        )

    def log_permission_denied(
        self,
        user_name: str | None,
        token: str | None,
        resource: str,
        action: str,
        reason: str | None = None,
    ) -> None:
        """Log permission denied event."""
        self.log(
            AuditEvent(
                timestamp=datetime.utcnow(),
                event_type=AuditEventType.PERMISSION_DENIED,
                user_name=user_name,
                user_token_prefix=self._get_token_prefix(token),
                resource=resource,
                action=action,
                status="denied",
                error=reason,
            )
        )

    def log_extraction(
        self,
        event_type: AuditEventType,
        user_name: str | None,
        job_id: str,
        country: str,
        queries: list[str],
        rows_extracted: int | None = None,
        error: str | None = None,
    ) -> None:
        """Log extraction event."""
        self.log(
            AuditEvent(
                timestamp=datetime.utcnow(),
                event_type=event_type,
                user_name=user_name,
                resource=job_id,
                action="extraction",
                status="success" if event_type == AuditEventType.EXTRACTION_COMPLETED else "started",
                details={
                    "country": country,
                    "queries": queries,
                    "rows_extracted": rows_extracted,
                },
                error=error,
            )
        )

    def log_delete_blocked(
        self,
        user_name: str | None,
        token: str | None,
        table: str,
        requested_rows: int,
        max_allowed: int,
    ) -> None:
        """Log blocked DELETE operation."""
        self.log(
            AuditEvent(
                timestamp=datetime.utcnow(),
                event_type=AuditEventType.DELETE_BLOCKED,
                user_name=user_name,
                user_token_prefix=self._get_token_prefix(token),
                resource=table,
                action="delete",
                status="blocked",
                details={
                    "requested_rows": requested_rows,
                    "max_allowed": max_allowed,
                },
            )
        )


# Global audit logger instance
audit_logger = AuditLogger()
