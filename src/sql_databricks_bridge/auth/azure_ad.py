"""Azure AD JWT token validation."""

import logging
import time
from dataclasses import dataclass, field

import httpx
import jwt
from jwt.algorithms import RSAAlgorithm

logger = logging.getLogger(__name__)

JWKS_CACHE_TTL_SECONDS = 86400  # 24 hours


@dataclass
class AzureADTokenClaims:
    """Validated claims extracted from an Azure AD JWT."""

    email: str
    name: str
    oid: str  # Azure AD object ID
    tid: str  # Tenant ID


@dataclass
class _JWKSCache:
    """Cached JWKS keys with expiration."""

    keys: dict = field(default_factory=dict)
    fetched_at: float = 0.0

    @property
    def is_expired(self) -> bool:
        return time.monotonic() - self.fetched_at > JWKS_CACHE_TTL_SECONDS


class AzureADTokenValidator:
    """Validates JWT tokens issued by Azure AD.

    Fetches JWKS signing keys from Azure AD and validates JWT signature,
    issuer, audience, and expiration.
    """

    def __init__(self, tenant_id: str, client_id: str) -> None:
        self.tenant_id = tenant_id
        self.client_id = client_id
        self._jwks_uri = (
            f"https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"
        )
        self._issuer = f"https://login.microsoftonline.com/{tenant_id}/v2.0"
        self._cache = _JWKSCache()

    def _fetch_jwks(self) -> dict:
        """Fetch JWKS keys from Azure AD, using cache when available."""
        if not self._cache.is_expired and self._cache.keys:
            return self._cache.keys

        logger.info("Fetching JWKS keys from Azure AD")
        response = httpx.get(self._jwks_uri, timeout=10)
        response.raise_for_status()

        jwks_data = response.json()

        # Build kid -> key mapping
        keys = {}
        for key_data in jwks_data.get("keys", []):
            kid = key_data.get("kid")
            if kid:
                keys[kid] = key_data

        self._cache = _JWKSCache(keys=keys, fetched_at=time.monotonic())
        logger.info(f"Cached {len(keys)} JWKS keys")
        return keys

    def _get_signing_key(self, token: str):
        """Get the signing key for a token based on its 'kid' header."""
        try:
            unverified_header = jwt.get_unverified_header(token)
        except jwt.DecodeError as e:
            raise InvalidTokenError(f"Cannot decode token header: {e}") from e

        kid = unverified_header.get("kid")
        if not kid:
            raise InvalidTokenError("Token header missing 'kid'")

        keys = self._fetch_jwks()
        key_data = keys.get(kid)

        if not key_data:
            # Key not found -- force refresh cache in case keys rotated
            self._cache.fetched_at = 0.0
            keys = self._fetch_jwks()
            key_data = keys.get(kid)

            if not key_data:
                raise InvalidTokenError(f"Signing key not found for kid: {kid}")

        return RSAAlgorithm.from_jwk(key_data)

    def validate(self, token: str) -> AzureADTokenClaims:
        """Validate an Azure AD JWT and return extracted claims.

        Args:
            token: Raw JWT string (without 'Bearer ' prefix).

        Returns:
            Validated token claims.

        Raises:
            InvalidTokenError: If the token is invalid, expired, or claims are missing.
        """
        public_key = self._get_signing_key(token)

        try:
            payload = jwt.decode(
                token,
                public_key,
                algorithms=["RS256"],
                audience=self.client_id,
                issuer=self._issuer,
                options={"require": ["exp", "iss", "aud", "oid"]},
            )
        except jwt.ExpiredSignatureError as e:
            raise InvalidTokenError("Token has expired") from e
        except jwt.InvalidAudienceError as e:
            raise InvalidTokenError("Invalid audience") from e
        except jwt.InvalidIssuerError as e:
            raise InvalidTokenError("Invalid issuer") from e
        except jwt.InvalidTokenError as e:
            raise InvalidTokenError(f"Invalid token: {e}") from e

        # Extract email -- try preferred_username first, then email, then upn
        email = (
            payload.get("preferred_username")
            or payload.get("email")
            or payload.get("upn")
        )
        if not email:
            raise InvalidTokenError(
                "Token missing email claim (preferred_username, email, or upn)"
            )

        name = payload.get("name", email.split("@")[0])

        return AzureADTokenClaims(
            email=email,
            name=name,
            oid=payload["oid"],
            tid=payload.get("tid", self.tenant_id),
        )


class InvalidTokenError(Exception):
    """Raised when a JWT token is invalid."""
