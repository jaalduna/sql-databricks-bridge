"""Unit tests for Azure AD JWT validation."""

import time
from unittest.mock import MagicMock, patch

import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

from sql_databricks_bridge.auth.azure_ad import (
    AzureADTokenClaims,
    AzureADTokenValidator,
    InvalidTokenError,
    _JWKSCache,
    JWKS_CACHE_TTL_SECONDS,
)


# --- Helpers to generate RSA keys and JWTs for testing ---

TENANT_ID = "test-tenant-id"
CLIENT_ID = "test-client-id"


def _generate_rsa_keypair():
    """Generate an RSA private key for test token signing."""
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key


def _private_key_to_jwk(private_key, kid: str = "test-kid") -> dict:
    """Convert an RSA private key to a JWK dict (public part only)."""
    from jwt.algorithms import RSAAlgorithm

    public_key = private_key.public_key()
    jwk = RSAAlgorithm.to_jwk(public_key, as_dict=True)
    jwk["kid"] = kid
    jwk["use"] = "sig"
    jwk["alg"] = "RS256"
    return jwk


def _create_test_token(
    private_key,
    kid: str = "test-kid",
    claims: dict | None = None,
    expired: bool = False,
) -> str:
    """Create a signed JWT for testing."""
    import time as _time

    now = int(_time.time())
    default_claims = {
        "iss": f"https://login.microsoftonline.com/{TENANT_ID}/v2.0",
        "aud": CLIENT_ID,
        "sub": "user-subject-id",
        "oid": "user-object-id",
        "tid": TENANT_ID,
        "preferred_username": "user@example.com",
        "name": "Test User",
        "iat": now - 300,
        "nbf": now - 300,
        "exp": now - 100 if expired else now + 3600,
    }
    if claims:
        default_claims.update(claims)

    return pyjwt.encode(
        default_claims,
        private_key,
        algorithm="RS256",
        headers={"kid": kid},
    )


@pytest.fixture
def rsa_keypair():
    """Generate an RSA key pair for test."""
    return _generate_rsa_keypair()


@pytest.fixture
def jwk_public(rsa_keypair):
    """JWK public key dict for the test key pair."""
    return _private_key_to_jwk(rsa_keypair, kid="test-kid")


@pytest.fixture
def validator():
    """Create a validator with test tenant/client IDs."""
    return AzureADTokenValidator(tenant_id=TENANT_ID, client_id=CLIENT_ID)


# --- Tests ---


class TestAzureADTokenClaims:
    """Tests for the AzureADTokenClaims dataclass."""

    def test_claims_fields(self):
        claims = AzureADTokenClaims(
            email="user@example.com",
            name="Test User",
            oid="obj-123",
            tid="tenant-456",
        )
        assert claims.email == "user@example.com"
        assert claims.name == "Test User"
        assert claims.oid == "obj-123"
        assert claims.tid == "tenant-456"


class TestJWKSCache:
    """Tests for the _JWKSCache helper."""

    def test_empty_cache_is_expired(self):
        cache = _JWKSCache()
        assert cache.is_expired is True

    def test_fresh_cache_is_not_expired(self):
        cache = _JWKSCache(keys={"k": "v"}, fetched_at=time.monotonic())
        assert cache.is_expired is False

    def test_old_cache_is_expired(self):
        old_time = time.monotonic() - JWKS_CACHE_TTL_SECONDS - 1
        cache = _JWKSCache(keys={"k": "v"}, fetched_at=old_time)
        assert cache.is_expired is True


class TestValidatorInit:
    """Tests for AzureADTokenValidator initialization."""

    def test_sets_jwks_uri(self, validator):
        assert TENANT_ID in validator._jwks_uri
        assert "discovery/v2.0/keys" in validator._jwks_uri

    def test_sets_issuer(self, validator):
        assert validator._issuer == f"https://login.microsoftonline.com/{TENANT_ID}/v2.0"


class TestFetchJWKS:
    """Tests for _fetch_jwks method."""

    def test_fetches_keys_from_azure(self, validator, jwk_public):
        """Fetch JWKS from Azure AD endpoint."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"keys": [jwk_public]}
        mock_response.raise_for_status = MagicMock()

        with patch("sql_databricks_bridge.auth.azure_ad.httpx.get", return_value=mock_response):
            keys = validator._fetch_jwks()

        assert "test-kid" in keys

    def test_uses_cache_when_fresh(self, validator, jwk_public):
        """Return cached keys without fetching when cache is fresh."""
        validator._cache = _JWKSCache(
            keys={"cached-kid": jwk_public},
            fetched_at=time.monotonic(),
        )

        with patch("sql_databricks_bridge.auth.azure_ad.httpx.get") as mock_get:
            keys = validator._fetch_jwks()

        mock_get.assert_not_called()
        assert "cached-kid" in keys

    def test_refetches_when_cache_expired(self, validator, jwk_public):
        """Refetch when cache has expired."""
        old_time = time.monotonic() - JWKS_CACHE_TTL_SECONDS - 1
        validator._cache = _JWKSCache(
            keys={"old-kid": {}},
            fetched_at=old_time,
        )

        mock_response = MagicMock()
        mock_response.json.return_value = {"keys": [jwk_public]}
        mock_response.raise_for_status = MagicMock()

        with patch("sql_databricks_bridge.auth.azure_ad.httpx.get", return_value=mock_response):
            keys = validator._fetch_jwks()

        assert "test-kid" in keys
        assert "old-kid" not in keys


class TestGetSigningKey:
    """Tests for _get_signing_key method."""

    def test_returns_key_for_known_kid(self, validator, rsa_keypair, jwk_public):
        """Return RSA key when kid is in the JWKS cache."""
        token = _create_test_token(rsa_keypair, kid="test-kid")
        validator._cache = _JWKSCache(
            keys={"test-kid": jwk_public},
            fetched_at=time.monotonic(),
        )

        key = validator._get_signing_key(token)
        assert key is not None

    def test_raises_for_malformed_token(self, validator):
        """Raise InvalidTokenError for token that cannot be decoded."""
        with pytest.raises(InvalidTokenError, match="Cannot decode token header"):
            validator._get_signing_key("not-a-jwt")

    def test_raises_for_missing_kid(self, validator):
        """Raise InvalidTokenError when token header has no kid."""
        # Create a token without kid header
        token = pyjwt.encode({"sub": "test"}, "secret", algorithm="HS256")

        with pytest.raises(InvalidTokenError, match="missing 'kid'"):
            validator._get_signing_key(token)

    def test_refetches_on_unknown_kid(self, validator, rsa_keypair, jwk_public):
        """Force cache refresh when kid is not found, then find it."""
        token = _create_test_token(rsa_keypair, kid="test-kid")

        # Cache has a different kid
        validator._cache = _JWKSCache(
            keys={"other-kid": {}},
            fetched_at=time.monotonic(),
        )

        mock_response = MagicMock()
        mock_response.json.return_value = {"keys": [jwk_public]}
        mock_response.raise_for_status = MagicMock()

        with patch("sql_databricks_bridge.auth.azure_ad.httpx.get", return_value=mock_response):
            key = validator._get_signing_key(token)

        assert key is not None

    def test_raises_when_kid_not_found_after_refresh(self, validator, rsa_keypair):
        """Raise InvalidTokenError when kid not found even after refresh."""
        token = _create_test_token(rsa_keypair, kid="unknown-kid")

        # Cache is empty
        validator._cache = _JWKSCache(keys={}, fetched_at=time.monotonic())

        mock_response = MagicMock()
        mock_response.json.return_value = {"keys": []}
        mock_response.raise_for_status = MagicMock()

        with patch("sql_databricks_bridge.auth.azure_ad.httpx.get", return_value=mock_response):
            with pytest.raises(InvalidTokenError, match="Signing key not found"):
                validator._get_signing_key(token)


class TestValidate:
    """Tests for the full validate() method."""

    def test_valid_token_returns_claims(self, validator, rsa_keypair, jwk_public):
        """Valid token returns correct claims."""
        token = _create_test_token(rsa_keypair, kid="test-kid")
        validator._cache = _JWKSCache(
            keys={"test-kid": jwk_public},
            fetched_at=time.monotonic(),
        )

        claims = validator.validate(token)

        assert isinstance(claims, AzureADTokenClaims)
        assert claims.email == "user@example.com"
        assert claims.name == "Test User"
        assert claims.oid == "user-object-id"
        assert claims.tid == TENANT_ID

    def test_expired_token_raises(self, validator, rsa_keypair, jwk_public):
        """Expired token raises InvalidTokenError."""
        token = _create_test_token(rsa_keypair, kid="test-kid", expired=True)
        validator._cache = _JWKSCache(
            keys={"test-kid": jwk_public},
            fetched_at=time.monotonic(),
        )

        with pytest.raises(InvalidTokenError, match="Token has expired"):
            validator.validate(token)

    def test_wrong_audience_raises(self, rsa_keypair, jwk_public):
        """Token with wrong audience raises InvalidTokenError."""
        validator = AzureADTokenValidator(
            tenant_id=TENANT_ID, client_id="different-client"
        )
        token = _create_test_token(rsa_keypair, kid="test-kid")
        validator._cache = _JWKSCache(
            keys={"test-kid": jwk_public},
            fetched_at=time.monotonic(),
        )

        with pytest.raises(InvalidTokenError, match="Invalid audience"):
            validator.validate(token)

    def test_wrong_issuer_raises(self, rsa_keypair, jwk_public):
        """Token with wrong issuer raises InvalidTokenError."""
        validator = AzureADTokenValidator(
            tenant_id="wrong-tenant", client_id=CLIENT_ID
        )
        token = _create_test_token(rsa_keypair, kid="test-kid")
        validator._cache = _JWKSCache(
            keys={"test-kid": jwk_public},
            fetched_at=time.monotonic(),
        )

        with pytest.raises(InvalidTokenError, match="Invalid issuer"):
            validator.validate(token)

    def test_email_from_preferred_username(self, validator, rsa_keypair, jwk_public):
        """Extract email from preferred_username claim."""
        token = _create_test_token(
            rsa_keypair,
            kid="test-kid",
            claims={"preferred_username": "pref@example.com"},
        )
        validator._cache = _JWKSCache(
            keys={"test-kid": jwk_public},
            fetched_at=time.monotonic(),
        )

        claims = validator.validate(token)
        assert claims.email == "pref@example.com"

    def test_email_from_email_claim(self, validator, rsa_keypair, jwk_public):
        """Fall back to email claim when preferred_username is missing."""
        token = _create_test_token(
            rsa_keypair,
            kid="test-kid",
            claims={"preferred_username": None, "email": "email@example.com"},
        )
        validator._cache = _JWKSCache(
            keys={"test-kid": jwk_public},
            fetched_at=time.monotonic(),
        )

        claims = validator.validate(token)
        assert claims.email == "email@example.com"

    def test_email_from_upn_claim(self, validator, rsa_keypair, jwk_public):
        """Fall back to upn claim when both preferred_username and email are missing."""
        token = _create_test_token(
            rsa_keypair,
            kid="test-kid",
            claims={
                "preferred_username": None,
                "email": None,
                "upn": "upn@example.com",
            },
        )
        validator._cache = _JWKSCache(
            keys={"test-kid": jwk_public},
            fetched_at=time.monotonic(),
        )

        claims = validator.validate(token)
        assert claims.email == "upn@example.com"

    def test_missing_email_raises(self, validator, rsa_keypair, jwk_public):
        """Raise InvalidTokenError when no email claim is present."""
        token = _create_test_token(
            rsa_keypair,
            kid="test-kid",
            claims={
                "preferred_username": None,
                "email": None,
                "upn": None,
            },
        )
        validator._cache = _JWKSCache(
            keys={"test-kid": jwk_public},
            fetched_at=time.monotonic(),
        )

        with pytest.raises(InvalidTokenError, match="missing email claim"):
            validator.validate(token)

    def test_name_defaults_to_email_prefix(self, validator, rsa_keypair, jwk_public):
        """Name defaults to email prefix when name claim is absent from the payload."""
        # Use a special claims dict that excludes the name key entirely
        import time as _time

        now = int(_time.time())
        payload = {
            "iss": f"https://login.microsoftonline.com/{TENANT_ID}/v2.0",
            "aud": CLIENT_ID,
            "sub": "user-subject-id",
            "oid": "user-object-id",
            "tid": TENANT_ID,
            "preferred_username": "someone@company.com",
            "iat": now - 300,
            "nbf": now - 300,
            "exp": now + 3600,
        }
        # Ensure 'name' key is NOT in payload (so .get() returns default)
        assert "name" not in payload

        token = pyjwt.encode(
            payload,
            rsa_keypair,
            algorithm="RS256",
            headers={"kid": "test-kid"},
        )
        validator._cache = _JWKSCache(
            keys={"test-kid": jwk_public},
            fetched_at=time.monotonic(),
        )

        claims = validator.validate(token)
        assert claims.name == "someone"


class TestInvalidTokenError:
    """Tests for InvalidTokenError."""

    def test_is_exception(self):
        err = InvalidTokenError("bad token")
        assert isinstance(err, Exception)
        assert str(err) == "bad token"
