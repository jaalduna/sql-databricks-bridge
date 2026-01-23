"""Token validation and management."""

import hashlib
import secrets
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class TokenInfo(BaseModel):
    """Information extracted from a validated token."""

    token_hash: str
    name: str
    created_at: datetime | None = None
    expires_at: datetime | None = None


def generate_token(prefix: str = "bridge") -> str:
    """Generate a new API token.

    Args:
        prefix: Token prefix for identification.

    Returns:
        Generated token string.
    """
    random_part = secrets.token_urlsafe(32)
    return f"{prefix}_{random_part}"


def hash_token(token: str) -> str:
    """Hash a token for secure storage.

    Args:
        token: Plain text token.

    Returns:
        SHA-256 hash of the token.
    """
    return hashlib.sha256(token.encode()).hexdigest()


def validate_token_format(token: str) -> bool:
    """Validate token format.

    Args:
        token: Token to validate.

    Returns:
        True if format is valid.
    """
    if not token:
        return False

    # Check minimum length
    if len(token) < 10:
        return False

    return True


def extract_bearer_token(authorization: str) -> Optional[str]:
    """Extract token from Authorization header.

    Args:
        authorization: Authorization header value.

    Returns:
        Token if found, None otherwise.
    """
    if not authorization:
        return None

    parts = authorization.split()

    if len(parts) != 2:
        return None

    scheme, token = parts

    if scheme.lower() != "bearer":
        return None

    return token
