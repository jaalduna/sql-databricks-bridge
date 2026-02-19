"""Unit tests for the authorized users allowlist and role-based access."""

import tempfile
from pathlib import Path

import pytest
import yaml

from sql_databricks_bridge.auth.authorized_users import (
    AuthorizedUser,
    AuthorizedUsersStore,
)


# --- AuthorizedUser model tests ---


class TestAuthorizedUser:
    """Tests for AuthorizedUser model."""

    def test_default_role_is_operator(self):
        """Default role should be 'operator'."""
        user = AuthorizedUser(email="user@test.com", name="User")
        assert user.roles == ["operator"]

    def test_is_admin_true(self):
        """is_admin returns True for admin role."""
        user = AuthorizedUser(
            email="admin@test.com", name="Admin", roles=["admin"]
        )
        assert user.is_admin is True

    def test_is_admin_false(self):
        """is_admin returns False for non-admin."""
        user = AuthorizedUser(
            email="user@test.com", name="User", roles=["operator"]
        )
        assert user.is_admin is False

    def test_is_admin_with_multiple_roles(self):
        """is_admin returns True when admin is one of multiple roles."""
        user = AuthorizedUser(
            email="user@test.com", name="User", roles=["operator", "admin"]
        )
        assert user.is_admin is True

    def test_can_trigger_sync_operator(self):
        """Operator can trigger syncs."""
        user = AuthorizedUser(
            email="op@test.com", name="Op", roles=["operator"]
        )
        assert user.can_trigger_sync is True

    def test_can_trigger_sync_admin(self):
        """Admin can trigger syncs."""
        user = AuthorizedUser(
            email="admin@test.com", name="Admin", roles=["admin"]
        )
        assert user.can_trigger_sync is True

    def test_can_trigger_sync_viewer_cannot(self):
        """Viewer cannot trigger syncs."""
        user = AuthorizedUser(
            email="viewer@test.com", name="Viewer", roles=["viewer"]
        )
        assert user.can_trigger_sync is False

    def test_can_trigger_sync_viewer_and_admin(self):
        """Admin role overrides viewer restriction."""
        user = AuthorizedUser(
            email="user@test.com", name="User", roles=["viewer", "admin"]
        )
        assert user.can_trigger_sync is True

    def test_can_access_country_specific(self):
        """User can access assigned countries."""
        user = AuthorizedUser(
            email="user@test.com",
            name="User",
            countries=["bolivia", "chile"],
        )
        assert user.can_access_country("bolivia") is True
        assert user.can_access_country("chile") is True
        assert user.can_access_country("brazil") is False

    def test_can_access_country_wildcard(self):
        """User with '*' can access all countries."""
        user = AuthorizedUser(
            email="user@test.com",
            name="User",
            countries=["*"],
        )
        assert user.can_access_country("bolivia") is True
        assert user.can_access_country("brazil") is True
        assert user.can_access_country("anything") is True

    def test_can_access_country_admin_all(self):
        """Admin can access all countries regardless of countries list."""
        user = AuthorizedUser(
            email="admin@test.com",
            name="Admin",
            roles=["admin"],
            countries=[],  # Empty list but admin overrides
        )
        assert user.can_access_country("bolivia") is True
        assert user.can_access_country("brazil") is True

    def test_can_access_country_empty_list(self):
        """User with empty countries list cannot access any country."""
        user = AuthorizedUser(
            email="user@test.com",
            name="User",
            roles=["operator"],
            countries=[],
        )
        assert user.can_access_country("bolivia") is False

    def test_default_countries_empty(self):
        """Default countries list is empty."""
        user = AuthorizedUser(email="user@test.com", name="User")
        assert user.countries == []


# --- AuthorizedUsersStore tests ---


class TestAuthorizedUsersStore:
    """Tests for AuthorizedUsersStore loading and lookup."""

    @pytest.fixture
    def users_yaml(self) -> str:
        """Create a temp YAML file with test users."""
        data = {
            "users": [
                {
                    "email": "admin@company.com",
                    "name": "Admin User",
                    "roles": ["admin"],
                    "countries": ["*"],
                },
                {
                    "email": "operator@company.com",
                    "name": "Operator",
                    "roles": ["operator"],
                    "countries": ["bolivia", "chile"],
                },
                {
                    "email": "viewer@company.com",
                    "name": "Viewer",
                    "roles": ["viewer"],
                    "countries": ["bolivia"],
                },
            ]
        }

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(data, f)
            return f.name

    @pytest.fixture
    def store(self, users_yaml) -> AuthorizedUsersStore:
        """Create and load a store from the test YAML."""
        s = AuthorizedUsersStore(users_yaml)
        s.load()
        return s

    def test_load_users_count(self, store):
        """Load correct number of users."""
        assert store.user_count == 3

    def test_get_user_by_email(self, store):
        """Look up user by email."""
        user = store.get_user("admin@company.com")
        assert user is not None
        assert user.name == "Admin User"
        assert user.is_admin is True

    def test_get_user_case_insensitive(self, store):
        """Email lookup is case-insensitive."""
        user = store.get_user("ADMIN@COMPANY.COM")
        assert user is not None
        assert user.name == "Admin User"

    def test_get_user_not_found(self, store):
        """Return None for unknown email."""
        user = store.get_user("unknown@company.com")
        assert user is None

    def test_operator_roles_and_countries(self, store):
        """Operator has correct roles and countries."""
        user = store.get_user("operator@company.com")
        assert user is not None
        assert user.roles == ["operator"]
        assert user.countries == ["bolivia", "chile"]
        assert user.can_trigger_sync is True
        assert user.can_access_country("bolivia") is True
        assert user.can_access_country("brazil") is False

    def test_viewer_cannot_trigger(self, store):
        """Viewer cannot trigger syncs."""
        user = store.get_user("viewer@company.com")
        assert user is not None
        assert user.can_trigger_sync is False

    def test_load_file_not_found(self):
        """Raise FileNotFoundError for missing config file."""
        store = AuthorizedUsersStore("/nonexistent/path.yaml")
        with pytest.raises(FileNotFoundError, match="Authorized users file not found"):
            store.load()

    def test_load_empty_file(self):
        """Handle empty YAML file gracefully."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write("")
            path = f.name

        try:
            store = AuthorizedUsersStore(path)
            store.load()
            assert store.user_count == 0
        finally:
            Path(path).unlink(missing_ok=True)

    def test_load_no_users_key(self):
        """Handle YAML with no 'users' key."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump({"other": "data"}, f)
            path = f.name

        try:
            store = AuthorizedUsersStore(path)
            store.load()
            assert store.user_count == 0
        finally:
            Path(path).unlink(missing_ok=True)

    def test_reload_clears_previous(self, users_yaml):
        """Reloading replaces previous users."""
        store = AuthorizedUsersStore(users_yaml)
        store.load()
        assert store.user_count == 3

        # Overwrite with fewer users
        with open(users_yaml, "w") as f:
            yaml.dump(
                {"users": [{"email": "only@test.com", "name": "Only"}]},
                f,
            )

        store.load()
        assert store.user_count == 1
        assert store.get_user("admin@company.com") is None
        assert store.get_user("only@test.com") is not None

        Path(users_yaml).unlink(missing_ok=True)
