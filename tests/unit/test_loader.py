"""Unit tests for permission loader."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from sql_databricks_bridge.auth.loader import PermissionLoader
from sql_databricks_bridge.auth.permissions import PermissionManager


@pytest.fixture
def permissions_yaml():
    """Sample permissions YAML content."""
    return {
        "users": [
            {
                "token": "token123",
                "name": "test_user",
                "permissions": [
                    {"table": "dbo.users", "access": "read"},
                    {"table": "dbo.orders", "access": "write"},
                    {"table": "dbo.admin", "access": "read_write", "max_delete_rows": 100},
                ],
            },
            {
                "token": "token456",
                "name": "another_user",
                "permissions": [
                    {"table": "dbo.users", "access": "read"},
                ],
            },
        ]
    }


@pytest.fixture
def permissions_file(permissions_yaml):
    """Create temporary permissions file."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yml", delete=False
    ) as f:
        yaml.dump(permissions_yaml, f)
        file_path = f.name

    yield Path(file_path)

    # Cleanup
    Path(file_path).unlink(missing_ok=True)


class TestPermissionLoader:
    """Tests for PermissionLoader class."""

    def test_load_permissions_file(self, permissions_file):
        """Load permissions from YAML file."""
        loader = PermissionLoader(permissions_file)
        manager = loader.load()

        assert isinstance(manager, PermissionManager)
        assert loader.manager is manager

    def test_load_file_not_found(self):
        """Raise error when file not found."""
        loader = PermissionLoader("/nonexistent/path.yml")

        with pytest.raises(FileNotFoundError):
            loader.load()

    def test_load_parses_users(self, permissions_file):
        """Parse all users from file."""
        loader = PermissionLoader(permissions_file)
        manager = loader.load()

        # Check both users were loaded
        user1 = manager.get_user("token123")
        user2 = manager.get_user("token456")

        assert user1 is not None
        assert user1.name == "test_user"
        assert user2 is not None
        assert user2.name == "another_user"

    def test_load_parses_permissions(self, permissions_file):
        """Parse permissions correctly."""
        loader = PermissionLoader(permissions_file)
        manager = loader.load()

        user = manager.get_user("token123")
        assert len(user.permissions) == 3

        # Check access types
        tables = {p.table: p for p in user.permissions}
        assert tables["dbo.users"].access.value == "read"
        assert tables["dbo.orders"].access.value == "write"
        assert tables["dbo.admin"].access.value == "read_write"

    def test_load_parses_max_delete_rows(self, permissions_file):
        """Parse max_delete_rows when present."""
        loader = PermissionLoader(permissions_file)
        manager = loader.load()

        user = manager.get_user("token123")
        tables = {p.table: p for p in user.permissions}

        assert tables["dbo.admin"].max_delete_rows == 100
        assert tables["dbo.users"].max_delete_rows is None

    def test_reload_permissions(self, permissions_file):
        """Force reload permissions."""
        loader = PermissionLoader(permissions_file)
        manager1 = loader.load()
        manager2 = loader.reload()

        # reload() calls load() which creates a new manager
        # But the loader.manager property points to the latest
        assert manager2 is loader.manager
        # Both managers should have the same users loaded
        assert manager1.get_user("token123") is not None
        assert manager2.get_user("token123") is not None

    def test_manager_property(self, permissions_file):
        """Manager property returns current manager."""
        loader = PermissionLoader(permissions_file)

        # Before loading
        assert loader.manager is None

        # After loading
        loader.load()
        assert loader.manager is not None

    def test_empty_users_list(self):
        """Handle empty users list."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            yaml.dump({"users": []}, f)
            file_path = f.name

        try:
            loader = PermissionLoader(file_path)
            manager = loader.load()
            assert manager is not None
        finally:
            Path(file_path).unlink(missing_ok=True)

    def test_empty_file(self):
        """Handle empty YAML file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write("")
            file_path = f.name

        try:
            loader = PermissionLoader(file_path)
            manager = loader.load()
            assert manager is not None
        finally:
            Path(file_path).unlink(missing_ok=True)

    def test_hot_reload_disabled_by_default(self, permissions_file):
        """Hot reload is disabled by default."""
        loader = PermissionLoader(permissions_file)
        assert loader.hot_reload is False

    def test_stop_watching_without_thread(self, permissions_file):
        """stop_watching handles no thread gracefully."""
        loader = PermissionLoader(permissions_file)
        # Should not raise
        loader.stop_watching()

    def test_on_reload_callback(self, permissions_file):
        """on_reload callback is stored."""
        callback = MagicMock()
        loader = PermissionLoader(
            permissions_file,
            on_reload=callback,
        )
        assert loader.on_reload is callback


class TestPermissionLoaderEdgeCases:
    """Edge case tests for PermissionLoader."""

    def test_default_access_when_missing(self):
        """Use default 'read' access when not specified."""
        yaml_content = {
            "users": [
                {
                    "token": "token123",
                    "name": "test_user",
                    "permissions": [
                        {"table": "dbo.users"},  # No access specified
                    ],
                },
            ]
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            yaml.dump(yaml_content, f)
            file_path = f.name

        try:
            loader = PermissionLoader(file_path)
            manager = loader.load()
            user = manager.get_user("token123")
            assert user.permissions[0].access.value == "read"
        finally:
            Path(file_path).unlink(missing_ok=True)

    def test_unknown_access_defaults_to_read(self):
        """Unknown access string defaults to read."""
        yaml_content = {
            "users": [
                {
                    "token": "token123",
                    "name": "test_user",
                    "permissions": [
                        {"table": "dbo.users", "access": "unknown_access"},
                    ],
                },
            ]
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            yaml.dump(yaml_content, f)
            file_path = f.name

        try:
            loader = PermissionLoader(file_path)
            manager = loader.load()
            user = manager.get_user("token123")
            assert user.permissions[0].access.value == "read"
        finally:
            Path(file_path).unlink(missing_ok=True)

    def test_user_without_permissions(self):
        """User without permissions list."""
        yaml_content = {
            "users": [
                {
                    "token": "token123",
                    "name": "test_user",
                    # No permissions key
                },
            ]
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            yaml.dump(yaml_content, f)
            file_path = f.name

        try:
            loader = PermissionLoader(file_path)
            manager = loader.load()
            user = manager.get_user("token123")
            assert user is not None
            assert len(user.permissions) == 0
        finally:
            Path(file_path).unlink(missing_ok=True)
