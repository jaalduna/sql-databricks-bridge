"""FastAPI dependencies for authentication and authorization."""

import logging
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from sql_databricks_bridge.api.schemas import UserPermissions
from sql_databricks_bridge.auth.audit import audit_logger
from sql_databricks_bridge.auth.loader import PermissionLoader
from sql_databricks_bridge.auth.permissions import PermissionManager
from sql_databricks_bridge.core.config import get_settings

logger = logging.getLogger(__name__)

# Security scheme
security = HTTPBearer(auto_error=False)

# Global permission manager
_permission_manager: PermissionManager | None = None
_permission_loader: PermissionLoader | None = None


def get_permission_manager() -> PermissionManager:
    """Get or initialize permission manager."""
    global _permission_manager, _permission_loader

    if _permission_manager is None:
        settings = get_settings()

        def on_reload(new_manager: PermissionManager) -> None:
            global _permission_manager
            _permission_manager = new_manager
            logger.info("Permissions reloaded")

        _permission_loader = PermissionLoader(
            permissions_file=settings.permissions_file,
            hot_reload=settings.permissions_hot_reload,
            on_reload=on_reload,
        )

        try:
            _permission_manager = _permission_loader.load()
        except FileNotFoundError:
            logger.warning(
                f"Permissions file not found: {settings.permissions_file}. "
                "Using empty permissions (all requests will be denied)."
            )
            _permission_manager = PermissionManager([])

    return _permission_manager


def get_client_ip(request: Request) -> str:
    """Extract client IP from request."""
    # Check for forwarded headers
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()

    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip

    return request.client.host if request.client else "unknown"


async def get_current_user(
    request: Request,
    credentials: Annotated[
        HTTPAuthorizationCredentials | None,
        Depends(security),
    ],
) -> UserPermissions:
    """Validate token and return current user.

    Args:
        request: FastAPI request.
        credentials: Bearer token credentials.

    Returns:
        User permissions for the authenticated user.

    Raises:
        HTTPException: If authentication fails.
    """
    settings = get_settings()

    # Skip auth if disabled
    if not settings.auth_enabled:
        return UserPermissions(
            token="disabled",
            name="auth_disabled",
            permissions=[],
        )

    client_ip = get_client_ip(request)

    # Check for credentials
    if not credentials:
        audit_logger.log_auth_failure(
            source_ip=client_ip,
            reason="No credentials provided",
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials
    manager = get_permission_manager()

    # Look up user
    user = manager.get_user(token)

    if not user:
        audit_logger.log_auth_failure(
            token=token,
            source_ip=client_ip,
            reason="Invalid token",
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    audit_logger.log_auth_success(
        user_name=user.name,
        token=token,
        source_ip=client_ip,
    )

    return user


async def get_optional_user(
    request: Request,
    credentials: Annotated[
        HTTPAuthorizationCredentials | None,
        Depends(security),
    ],
) -> UserPermissions | None:
    """Get current user if authenticated, None otherwise.

    Use this for endpoints that work differently for authenticated vs anonymous users.
    """
    settings = get_settings()

    if not settings.auth_enabled:
        return None

    if not credentials:
        return None

    token = credentials.credentials
    manager = get_permission_manager()

    return manager.get_user(token)


# Type alias for dependency injection
CurrentUser = Annotated[UserPermissions, Depends(get_current_user)]
OptionalUser = Annotated[UserPermissions | None, Depends(get_optional_user)]
