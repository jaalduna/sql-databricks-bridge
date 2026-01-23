"""Unit tests for auth modules."""

import logging
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sql_databricks_bridge.auth.audit import (
    AuditEvent,
    AuditEventType,
    AuditLogger,
)
from sql_databricks_bridge.auth.token import (
    TokenInfo,
    extract_bearer_token,
    generate_token,
    hash_token,
    validate_token_format,
)


class TestTokenFunctions:
    """Tests for token.py functions."""

    def test_generate_token_default_prefix(self):
        """Generate token with default prefix."""
        token = generate_token()
        assert token.startswith("bridge_")
        assert len(token) > 20

    def test_generate_token_custom_prefix(self):
        """Generate token with custom prefix."""
        token = generate_token(prefix="api")
        assert token.startswith("api_")

    def test_generate_token_uniqueness(self):
        """Each generated token is unique."""
        tokens = [generate_token() for _ in range(10)]
        assert len(set(tokens)) == 10

    def test_hash_token(self):
        """Hash token returns consistent hash."""
        token = "test_token_123"
        hash1 = hash_token(token)
        hash2 = hash_token(token)
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA-256 hex

    def test_hash_token_different_inputs(self):
        """Different tokens have different hashes."""
        hash1 = hash_token("token1")
        hash2 = hash_token("token2")
        assert hash1 != hash2

    def test_validate_token_format_valid(self):
        """Valid tokens pass validation."""
        assert validate_token_format("bridge_abc123xyz") is True
        assert validate_token_format("api_longenoughtoken") is True

    def test_validate_token_format_empty(self):
        """Empty token fails validation."""
        assert validate_token_format("") is False

    def test_validate_token_format_too_short(self):
        """Short token fails validation."""
        assert validate_token_format("short") is False

    def test_extract_bearer_token_valid(self):
        """Extract token from valid Bearer header."""
        token = extract_bearer_token("Bearer abc123token")
        assert token == "abc123token"

    def test_extract_bearer_token_case_insensitive(self):
        """Bearer scheme is case insensitive."""
        token = extract_bearer_token("bearer abc123")
        assert token == "abc123"

        token = extract_bearer_token("BEARER abc123")
        assert token == "abc123"

    def test_extract_bearer_token_empty(self):
        """Empty header returns None."""
        assert extract_bearer_token("") is None

    def test_extract_bearer_token_no_scheme(self):
        """Missing scheme returns None."""
        assert extract_bearer_token("abc123") is None

    def test_extract_bearer_token_wrong_scheme(self):
        """Wrong scheme returns None."""
        assert extract_bearer_token("Basic abc123") is None

    def test_extract_bearer_token_extra_parts(self):
        """Too many parts returns None."""
        assert extract_bearer_token("Bearer abc 123") is None


class TestTokenInfo:
    """Tests for TokenInfo model."""

    def test_token_info_minimal(self):
        """Create TokenInfo with minimal fields."""
        info = TokenInfo(token_hash="abc123", name="test")
        assert info.token_hash == "abc123"
        assert info.name == "test"
        assert info.created_at is None
        assert info.expires_at is None

    def test_token_info_full(self):
        """Create TokenInfo with all fields."""
        now = datetime.now()
        info = TokenInfo(
            token_hash="abc123",
            name="test",
            created_at=now,
            expires_at=now,
        )
        assert info.created_at == now
        assert info.expires_at == now


class TestAuditEventType:
    """Tests for AuditEventType enum."""

    def test_all_event_types_exist(self):
        """All expected event types are defined."""
        assert AuditEventType.AUTH_SUCCESS == "auth_success"
        assert AuditEventType.AUTH_FAILURE == "auth_failure"
        assert AuditEventType.PERMISSION_GRANTED == "permission_granted"
        assert AuditEventType.PERMISSION_DENIED == "permission_denied"
        assert AuditEventType.EXTRACTION_STARTED == "extraction_started"
        assert AuditEventType.EXTRACTION_COMPLETED == "extraction_completed"
        assert AuditEventType.EXTRACTION_FAILED == "extraction_failed"
        assert AuditEventType.SYNC_STARTED == "sync_started"
        assert AuditEventType.SYNC_COMPLETED == "sync_completed"
        assert AuditEventType.SYNC_FAILED == "sync_failed"
        assert AuditEventType.DELETE_BLOCKED == "delete_blocked"


class TestAuditEvent:
    """Tests for AuditEvent model."""

    def test_audit_event_minimal(self):
        """Create event with required fields only."""
        event = AuditEvent(
            timestamp=datetime.now(),
            event_type=AuditEventType.AUTH_SUCCESS,
        )
        assert event.user_name is None
        assert event.details is None

    def test_audit_event_full(self):
        """Create event with all fields."""
        event = AuditEvent(
            timestamp=datetime.now(),
            event_type=AuditEventType.EXTRACTION_COMPLETED,
            user_name="test_user",
            user_token_prefix="bridge_a...",
            source_ip="192.168.1.1",
            resource="job-123",
            action="extract",
            status="success",
            details={"rows": 100},
            error=None,
        )
        assert event.user_name == "test_user"
        assert event.details["rows"] == 100


class TestAuditLogger:
    """Tests for AuditLogger class."""

    @pytest.fixture
    def logger(self):
        """Create audit logger without file."""
        return AuditLogger()

    def test_get_token_prefix_valid(self, logger):
        """Get prefix from valid token."""
        prefix = logger._get_token_prefix("bridge_abcdefghijk")
        assert prefix == "bridge_a..."

    def test_get_token_prefix_none(self, logger):
        """Get prefix from None token."""
        prefix = logger._get_token_prefix(None)
        assert prefix is None

    def test_log_auth_success(self, logger, caplog):
        """Log successful authentication."""
        with caplog.at_level(logging.INFO):
            logger.log_auth_success(
                user_name="test_user",
                token="bridge_token123",
                source_ip="192.168.1.1",
            )

        assert "auth_success" in caplog.text

    def test_log_auth_failure(self, logger, caplog):
        """Log failed authentication."""
        with caplog.at_level(logging.WARNING):
            logger.log_auth_failure(
                token="invalid_token",
                source_ip="192.168.1.1",
                reason="Invalid token",
            )

        assert "auth_failure" in caplog.text

    def test_log_permission_denied(self, logger, caplog):
        """Log permission denied."""
        with caplog.at_level(logging.WARNING):
            logger.log_permission_denied(
                user_name="test_user",
                token="bridge_token",
                resource="dbo.users",
                action="write",
                reason="No write access",
            )

        assert "permission_denied" in caplog.text

    def test_log_extraction(self, logger, caplog):
        """Log extraction event."""
        with caplog.at_level(logging.INFO):
            logger.log_extraction(
                event_type=AuditEventType.EXTRACTION_COMPLETED,
                user_name="test_user",
                job_id="job-123",
                country="CL",
                queries=["query1", "query2"],
                rows_extracted=1000,
            )

        assert "extraction_completed" in caplog.text

    def test_log_delete_blocked(self, logger, caplog):
        """Log blocked DELETE operation."""
        with caplog.at_level(logging.WARNING):
            logger.log_delete_blocked(
                user_name="test_user",
                token="bridge_token",
                table="dbo.users",
                requested_rows=1000,
                max_allowed=100,
            )

        assert "delete_blocked" in caplog.text
        assert "1000" in caplog.text

    def test_log_to_file(self):
        """Log events to file - verify file logger is configured."""
        import logging as log_module

        with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
            file_path = f.name

        try:
            audit_logger = AuditLogger(log_to_file=file_path)

            # Check that a file handler was added
            audit_log = log_module.getLogger("sql_databricks_bridge.audit")

            # The audit logger adds a file handler when log_to_file is provided
            assert audit_logger.log_to_file == file_path

            # Verify the file handler was added
            file_handlers = [h for h in audit_log.handlers if isinstance(h, log_module.FileHandler)]
            assert len(file_handlers) > 0, "File handler should be added"

        finally:
            Path(file_path).unlink(missing_ok=True)
            # Clean up handlers to avoid affecting other tests
            audit_log = log_module.getLogger("sql_databricks_bridge.audit")
            audit_log.handlers = [
                h for h in audit_log.handlers
                if not isinstance(h, log_module.FileHandler)
            ]

    def test_log_levels(self, logger, caplog):
        """Different event types use different log levels."""
        # Error for failed events
        with caplog.at_level(logging.ERROR):
            event = AuditEvent(
                timestamp=datetime.now(),
                event_type=AuditEventType.EXTRACTION_FAILED,
            )
            logger.log(event)

        assert len([r for r in caplog.records if r.levelno == logging.ERROR]) == 1
