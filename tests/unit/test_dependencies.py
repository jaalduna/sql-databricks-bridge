"""Unit tests for FastAPI dependencies."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from sql_databricks_bridge.api.dependencies import (
    get_client_ip,
    get_current_user,
    get_optional_user,
    get_permission_manager,
)
from sql_databricks_bridge.api.schemas import PermissionAccess, TablePermission, UserPermissions
from sql_databricks_bridge.auth.permissions import PermissionManager


@pytest.fixture
def mock_request():
    """Create mock FastAPI request."""
    request = MagicMock()
    request.client.host = "192.168.1.100"
    request.headers = {}
    return request


@pytest.fixture
def mock_credentials():
    """Create mock HTTP credentials."""
    creds = MagicMock(spec=HTTPAuthorizationCredentials)
    creds.credentials = "valid_token_123"
    return creds


@pytest.fixture
def mock_user():
    """Create mock user permissions."""
    return UserPermissions(
        token="valid_token_123",
        name="test_user",
        permissions=[
            TablePermission(table="dbo.users", access=PermissionAccess.READ),
        ],
    )


class TestGetClientIp:
    """Tests for get_client_ip function."""

    def test_client_ip_direct(self, mock_request):
        """Get IP from direct connection."""
        ip = get_client_ip(mock_request)
        assert ip == "192.168.1.100"

    def test_client_ip_forwarded(self, mock_request):
        """Get IP from x-forwarded-for header."""
        mock_request.headers = {"x-forwarded-for": "10.0.0.1, 192.168.1.1"}
        ip = get_client_ip(mock_request)
        assert ip == "10.0.0.1"

    def test_client_ip_real_ip_header(self, mock_request):
        """Get IP from x-real-ip header."""
        mock_request.headers = {"x-real-ip": "10.0.0.2"}
        ip = get_client_ip(mock_request)
        assert ip == "10.0.0.2"

    def test_client_ip_no_client(self, mock_request):
        """Handle missing client."""
        mock_request.client = None
        ip = get_client_ip(mock_request)
        assert ip == "unknown"


class TestGetCurrentUser:
    """Tests for get_current_user dependency."""

    @pytest.mark.asyncio
    async def test_auth_disabled(self, mock_request):
        """Skip auth when disabled."""
        with patch("sql_databricks_bridge.api.dependencies.get_settings") as mock_settings:
            mock_settings.return_value.auth_enabled = False

            user = await get_current_user(mock_request, None)

            assert user.name == "auth_disabled"
            assert user.token == "disabled"

    @pytest.mark.asyncio
    async def test_no_credentials(self, mock_request):
        """Raise 401 when no credentials provided."""
        with patch("sql_databricks_bridge.api.dependencies.get_settings") as mock_settings:
            mock_settings.return_value.auth_enabled = True

            with pytest.raises(HTTPException) as exc_info:
                await get_current_user(mock_request, None)

            assert exc_info.value.status_code == 401
            assert "Not authenticated" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_invalid_token(self, mock_request, mock_credentials):
        """Raise 401 for invalid token."""
        with patch("sql_databricks_bridge.api.dependencies.get_settings") as mock_settings:
            mock_settings.return_value.auth_enabled = True

            with patch(
                "sql_databricks_bridge.api.dependencies.get_permission_manager"
            ) as mock_get_pm:
                mock_manager = MagicMock()
                mock_manager.get_user.return_value = None
                mock_get_pm.return_value = mock_manager

                with pytest.raises(HTTPException) as exc_info:
                    await get_current_user(mock_request, mock_credentials)

                assert exc_info.value.status_code == 401
                assert "Invalid authentication token" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_valid_token(self, mock_request, mock_credentials, mock_user):
        """Return user for valid token."""
        with patch("sql_databricks_bridge.api.dependencies.get_settings") as mock_settings:
            mock_settings.return_value.auth_enabled = True

            with patch(
                "sql_databricks_bridge.api.dependencies.get_permission_manager"
            ) as mock_get_pm:
                mock_manager = MagicMock()
                mock_manager.get_user.return_value = mock_user
                mock_get_pm.return_value = mock_manager

                user = await get_current_user(mock_request, mock_credentials)

                assert user.name == "test_user"


class TestGetOptionalUser:
    """Tests for get_optional_user dependency."""

    @pytest.mark.asyncio
    async def test_auth_disabled(self, mock_request):
        """Return None when auth disabled."""
        with patch("sql_databricks_bridge.api.dependencies.get_settings") as mock_settings:
            mock_settings.return_value.auth_enabled = False

            user = await get_optional_user(mock_request, None)

            assert user is None

    @pytest.mark.asyncio
    async def test_no_credentials(self, mock_request):
        """Return None when no credentials."""
        with patch("sql_databricks_bridge.api.dependencies.get_settings") as mock_settings:
            mock_settings.return_value.auth_enabled = True

            user = await get_optional_user(mock_request, None)

            assert user is None

    @pytest.mark.asyncio
    async def test_valid_token(self, mock_request, mock_credentials, mock_user):
        """Return user for valid token."""
        with patch("sql_databricks_bridge.api.dependencies.get_settings") as mock_settings:
            mock_settings.return_value.auth_enabled = True

            with patch(
                "sql_databricks_bridge.api.dependencies.get_permission_manager"
            ) as mock_get_pm:
                mock_manager = MagicMock()
                mock_manager.get_user.return_value = mock_user
                mock_get_pm.return_value = mock_manager

                user = await get_optional_user(mock_request, mock_credentials)

                assert user.name == "test_user"


class TestGetPermissionManager:
    """Tests for get_permission_manager function."""

    def test_creates_manager_from_file(self):
        """Create manager from permissions file."""
        import tempfile
        from pathlib import Path

        import yaml

        # Create temp permissions file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            yaml.dump({"users": []}, f)
            file_path = f.name

        try:
            with patch("sql_databricks_bridge.api.dependencies.get_settings") as mock_settings:
                mock_settings.return_value.permissions_file = file_path
                mock_settings.return_value.permissions_hot_reload = False

                # Reset global state
                import sql_databricks_bridge.api.dependencies as deps

                deps._permission_manager = None
                deps._permission_loader = None

                manager = get_permission_manager()

                assert isinstance(manager, PermissionManager)
        finally:
            Path(file_path).unlink(missing_ok=True)
            # Reset global state
            import sql_databricks_bridge.api.dependencies as deps

            deps._permission_manager = None
            deps._permission_loader = None

    def test_returns_cached_manager(self):
        """Return cached manager on subsequent calls."""
        with patch("sql_databricks_bridge.api.dependencies.get_settings") as mock_settings:
            # Reset global state
            import sql_databricks_bridge.api.dependencies as deps

            deps._permission_manager = PermissionManager([])
            deps._permission_loader = MagicMock()

            manager1 = get_permission_manager()
            manager2 = get_permission_manager()

            assert manager1 is manager2

            # Cleanup
            deps._permission_manager = None
            deps._permission_loader = None

    def test_handles_missing_file(self):
        """Handle missing permissions file gracefully."""
        with patch("sql_databricks_bridge.api.dependencies.get_settings") as mock_settings:
            mock_settings.return_value.permissions_file = "/nonexistent/path.yml"
            mock_settings.return_value.permissions_hot_reload = False

            # Reset global state
            import sql_databricks_bridge.api.dependencies as deps

            deps._permission_manager = None
            deps._permission_loader = None

            manager = get_permission_manager()

            # Should return empty manager
            assert isinstance(manager, PermissionManager)

            # Cleanup
            deps._permission_manager = None
            deps._permission_loader = None
