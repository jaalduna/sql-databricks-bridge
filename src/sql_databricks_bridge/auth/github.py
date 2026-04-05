"""GitHub OAuth helpers: token exchange, JWT sign/verify, and permission resolution."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import httpx
import jwt

logger = logging.getLogger(__name__)


class GitHubOAuthError(Exception):
    """Raised when the GitHub OAuth flow fails."""


def resolve_user_permissions(email: str, admin_emails: str) -> dict[str, list[str]]:
    """Resolve roles and countries for a GitHub-authenticated user.

    If admin_emails is empty, any authenticated GitHub user gets admin + all countries.
    Otherwise only listed emails get admin; everyone else gets viewer with no countries.
    """
    whitelist = [e.strip().lower() for e in admin_emails.split(",") if e.strip()] if admin_emails else []
    if not whitelist or email.lower() in whitelist:
        return {"roles": ["admin"], "countries": ["*"]}
    return {"roles": ["viewer"], "countries": []}


async def exchange_github_code(
    code: str,
    client_id: str,
    client_secret: str,
    callback_url: str,
) -> dict[str, str]:
    """Exchange a GitHub OAuth authorization code for user profile data.

    Returns:
        dict with ``email`` and ``name`` keys.

    Raises:
        GitHubOAuthError: If GitHub returns an error or the exchange fails.
    """
    async with httpx.AsyncClient(timeout=15.0) as client:
        token_resp = await client.post(
            "https://github.com/login/oauth/access_token",
            json={
                "client_id": client_id,
                "client_secret": client_secret,
                "code": code,
                "redirect_uri": callback_url,
            },
            headers={"Accept": "application/json"},
        )
        token_data = token_resp.json()

    if "error" in token_data:
        raise GitHubOAuthError(
            f"{token_data['error']}: {token_data.get('error_description', '')}"
        )

    gh_token = token_data["access_token"]
    gh_headers = {
        "Authorization": f"Bearer {gh_token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        user_resp, emails_resp = await asyncio.gather(
            client.get("https://api.github.com/user", headers=gh_headers),
            client.get("https://api.github.com/user/emails", headers=gh_headers),
        )

    gh_user = user_resp.json()
    gh_emails = emails_resp.json()

    # Prefer primary verified email; fall back to public profile email or noreply alias
    primary_email: str | None = None
    if isinstance(gh_emails, list):
        for entry in gh_emails:
            if entry.get("primary") and entry.get("verified"):
                primary_email = entry["email"]
                break
    primary_email = (
        primary_email
        or gh_user.get("email")
        or f"{gh_user['login']}@users.noreply.github.com"
    )

    return {
        "email": primary_email,
        "name": gh_user.get("name") or gh_user.get("login", ""),
    }


def sign_github_jwt(
    email: str,
    name: str,
    roles: list[str],
    countries: list[str],
    secret: str,
    expiry_hours: int = 8,
) -> str:
    """Sign an app JWT for a GitHub-authenticated user."""
    payload = {
        "email": email,
        "name": name,
        "roles": roles,
        "countries": countries,
        "iss": "sql-databricks-bridge",
        "exp": datetime.now(timezone.utc) + timedelta(hours=expiry_hours),
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def verify_github_jwt(token: str, secret: str) -> dict | None:
    """Verify and decode a GitHub app JWT.

    Returns:
        Claims dict if valid, None if invalid or expired.
    """
    try:
        return jwt.decode(token, secret, algorithms=["HS256"])
    except jwt.PyJWTError:
        return None
