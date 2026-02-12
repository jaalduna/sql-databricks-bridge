"""FastAPI dependencies for authentication and authorization."""

import logging
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from sql_databricks_bridge.api.schemas import UserPermissions
from sql_databricks_bridge.auth.audit import audit_logger
from sql_databricks_bridge.auth.authorized_users import AuthorizedUser, AuthorizedUsersStore
from sql_databricks_bridge.auth.azure_ad import AzureADTokenValidator, InvalidTokenError
from sql_databricks_bridge.auth.loader import PermissionLoader
from sql_databricks_bridge.auth.permissions import PermissionManager
from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.core.paths import get_config_file

logger = logging.getLogger(__name__)

# Security scheme
security = HTTPBearer(auto_error=False)

# Global permission manager
_permission_manager: PermissionManager | None = None
_permission_loader: PermissionLoader | None = None

# Global Azure AD validator and authorized users store
_azure_ad_validator: AzureADTokenValidator | None = None
_authorized_users_store: AuthorizedUsersStore | None = None


def get_permission_manager() -> PermissionManager:
    """Get or initialize permission manager."""
    global _permission_manager, _permission_loader

    if _permission_manager is None:
        settings = get_settings()

        def on_reload(new_manager: PermissionManager) -> None:
            global _permission_manager
            _permission_manager = new_manager
            logger.info("Permissions reloaded")

        permissions_path = (
            settings.permissions_file
            if settings.permissions_file
            else str(get_config_file("permissions.yaml"))
        )

        _permission_loader = PermissionLoader(
            permissions_file=permissions_path,
            hot_reload=settings.permissions_hot_reload,
            on_reload=on_reload,
        )

        try:
            _permission_manager = _permission_loader.load()
        except FileNotFoundError:
            logger.warning(
                f"Permissions file not found: {permissions_path}. "
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


# Type alias for dependency injection (existing token-based auth)
CurrentUser = Annotated[UserPermissions, Depends(get_current_user)]
OptionalUser = Annotated[UserPermissions | None, Depends(get_optional_user)]


# --- Azure AD JWT auth (for frontend) ---


def get_azure_ad_validator() -> AzureADTokenValidator:
    """Get or initialize Azure AD token validator."""
    global _azure_ad_validator

    if _azure_ad_validator is None:
        settings = get_settings()
        if not settings.azure_ad_tenant_id or not settings.azure_ad_client_id:
            raise RuntimeError(
                "Azure AD not configured. Set AZURE_AD_TENANT_ID and AZURE_AD_CLIENT_ID."
            )
        _azure_ad_validator = AzureADTokenValidator(
            tenant_id=settings.azure_ad_tenant_id,
            client_id=settings.azure_ad_client_id,
        )

    return _azure_ad_validator


def get_authorized_users_store() -> AuthorizedUsersStore:
    """Get or initialize authorized users store."""
    global _authorized_users_store

    if _authorized_users_store is None:
        settings = get_settings()
        users_path = (
            settings.authorized_users_file
            if settings.authorized_users_file
            else str(get_config_file("authorized_users.yaml"))
        )
        _authorized_users_store = AuthorizedUsersStore(users_path)
        try:
            _authorized_users_store.load()
        except FileNotFoundError:
            logger.warning(
                f"Authorized users file not found: {users_path}. "
                "All Azure AD users will be denied."
            )

    return _authorized_users_store


async def get_current_azure_ad_user(
    request: Request,
    credentials: Annotated[
        HTTPAuthorizationCredentials | None,
        Depends(security),
    ],
) -> AuthorizedUser:
    """Validate Azure AD JWT and return the authorized user.

    This dependency validates the JWT token against Azure AD JWKS keys,
    then checks the user's email against the authorized users allowlist.

    Returns:
        AuthorizedUser from the allowlist.

    Raises:
        HTTPException: 401 if token invalid/expired, 403 if user not authorized.
    """
    settings = get_settings()
    client_ip = get_client_ip(request)

    if not settings.auth_enabled:
        return AuthorizedUser(
            email="auth_disabled@local",
            name="Auth Disabled",
            roles=["admin"],
            countries=["*"],
        )

    if not credentials:
        audit_logger.log_auth_failure(
            source_ip=client_ip,
            reason="No credentials provided",
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "unauthorized", "message": "Not authenticated"},
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials

    # Validate JWT against Azure AD
    validator = get_azure_ad_validator()
    try:
        claims = validator.validate(token)
    except InvalidTokenError as e:
        audit_logger.log_auth_failure(
            token=token,
            source_ip=client_ip,
            reason=str(e),
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "unauthorized", "message": str(e)},
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Check allowlist
    store = get_authorized_users_store()
    user = store.get_user(claims.email)

    if not user:
        audit_logger.log_auth_failure(
            token=token,
            source_ip=client_ip,
            reason=f"User not in allowlist: {claims.email}",
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "forbidden",
                "message": "User not in authorized allowlist",
            },
        )

    audit_logger.log_auth_success(
        user_name=user.name,
        token=token,
        source_ip=client_ip,
    )

    return user


# Type alias for Azure AD dependency
CurrentAzureADUser = Annotated[AuthorizedUser, Depends(get_current_azure_ad_user)]
