"""Auth API endpoints for frontend integration."""

import logging

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from sql_databricks_bridge.api.dependencies import CurrentAzureADUser
from sql_databricks_bridge.auth.github import (
    GitHubOAuthError,
    exchange_github_code,
    resolve_user_permissions,
    sign_github_jwt,
    verify_github_jwt,
)
from sql_databricks_bridge.core.config import get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# /api/v1/auth — versioned router (mounted under /api/v1 prefix)
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/auth", tags=["Auth"])


class UserInfoResponse(BaseModel):
    """User info returned by GET /auth/me."""

    email: str
    name: str
    roles: list[str]
    countries: list[str]


@router.get(
    "/me",
    response_model=UserInfoResponse,
    summary="Get current user info",
    description=(
        "Returns the authenticated user's identity, roles, and authorized countries. "
        "Accepts both Azure AD tokens and GitHub app JWTs."
    ),
)
async def get_me(request: Request, user: CurrentAzureADUser) -> UserInfoResponse:
    """Return current authenticated user's info."""
    return UserInfoResponse(
        email=user.email,
        name=user.name,
        roles=user.roles,
        countries=user.countries,
    )


# ---------------------------------------------------------------------------
# /auth — root-level router for GitHub OAuth (mounted directly on app, NOT
# under /api/v1 so the frontend can redirect to {serverRoot}/auth/github).
# ---------------------------------------------------------------------------

github_router = APIRouter(tags=["GitHub OAuth"])


@github_router.get(
    "/auth/github",
    summary="Initiate GitHub OAuth flow",
    description="Redirects the browser to the GitHub OAuth authorization page.",
    response_class=RedirectResponse,
)
async def github_login() -> RedirectResponse:
    """Redirect to GitHub OAuth authorization."""
    settings = get_settings()

    if not settings.github_client_id:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "github_not_configured", "message": "GITHUB_CLIENT_ID is not set"},
        )

    from urllib.parse import urlencode

    params = urlencode({
        "client_id": settings.github_client_id,
        "redirect_uri": settings.github_callback_url,
        "scope": "user:email",
    })
    return RedirectResponse(url=f"https://github.com/login/oauth/authorize?{params}")


@github_router.get(
    "/auth/github/callback",
    summary="GitHub OAuth callback",
    description=(
        "Receives the GitHub authorization code, exchanges it for a user profile, "
        "issues an app JWT, and redirects to the frontend with ?token=<jwt>."
    ),
    response_class=RedirectResponse,
)
async def github_callback(code: str | None = None) -> RedirectResponse:
    """Handle GitHub OAuth callback: exchange code → issue JWT → redirect to frontend."""
    settings = get_settings()

    if not code:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "missing_code", "message": "No authorization code in callback"},
        )

    try:
        profile = await exchange_github_code(
            code=code,
            client_id=settings.github_client_id,
            client_secret=settings.github_client_secret.get_secret_value(),
            callback_url=settings.github_callback_url,
        )
    except GitHubOAuthError as exc:
        logger.error("GitHub OAuth code exchange failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "oauth_exchange_failed", "message": str(exc)},
        ) from exc
    except Exception as exc:
        logger.exception("Unexpected error during GitHub OAuth callback")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "oauth_callback_failed", "message": str(exc)},
        ) from exc

    permissions = resolve_user_permissions(profile["email"], settings.admin_emails)
    token = sign_github_jwt(
        email=profile["email"],
        name=profile["name"],
        roles=permissions["roles"],
        countries=permissions["countries"],
        secret=settings.jwt_secret.get_secret_value(),
        expiry_hours=settings.jwt_expiry_hours,
    )

    from urllib.parse import urlencode

    frontend_url = settings.frontend_url.rstrip("/")
    redirect_url = f"{frontend_url}/?{urlencode({'token': token})}"
    logger.info("GitHub OAuth success for %s — redirecting to frontend", profile["email"])
    return RedirectResponse(url=redirect_url)
