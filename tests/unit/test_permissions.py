"""Unit tests for permission management."""

import pytest

from sql_databricks_bridge.api.schemas import (
    PermissionAccess,
    TablePermission,
    UserPermissions,
)
from sql_databricks_bridge.auth.permissions import PermissionDeniedError, PermissionManager


@pytest.fixture
def sample_users() -> list[UserPermissions]:
    """Create sample users for testing."""
    return [
        UserPermissions(
            token="user1-token",
            name="user1",
            permissions=[
                TablePermission(table="dbo.Table1", access=PermissionAccess.READ),
                TablePermission(table="dbo.Table2", access=PermissionAccess.WRITE),
                TablePermission(
                    table="dbo.Table3",
                    access=PermissionAccess.READ_WRITE,
                    max_delete_rows=1000,
                ),
            ],
        ),
        UserPermissions(
            token="user2-token",
            name="user2",
            permissions=[
                TablePermission(table="dbo.Table1", access=PermissionAccess.READ_WRITE),
            ],
        ),
    ]


@pytest.fixture
def manager(sample_users: list[UserPermissions]) -> PermissionManager:
    """Create permission manager with sample users."""
    return PermissionManager(sample_users)


class TestPermissionManager:
    """Tests for PermissionManager class."""

    def test_get_user(self, manager: PermissionManager):
        """Test getting user by token."""
        user = manager.get_user("user1-token")
        assert user is not None
        assert user.name == "user1"

        user = manager.get_user("nonexistent")
        assert user is None

    def test_has_read_permission(self, manager: PermissionManager):
        """Test read permission checks."""
        # User1 has read on Table1
        assert manager.has_permission("user1-token", "dbo.Table1", PermissionAccess.READ)

        # User1 has write on Table2 (not read)
        assert not manager.has_permission("user1-token", "dbo.Table2", PermissionAccess.READ)

        # User1 has read_write on Table3 (includes read)
        assert manager.has_permission("user1-token", "dbo.Table3", PermissionAccess.READ)

    def test_has_write_permission(self, manager: PermissionManager):
        """Test write permission checks."""
        # User1 has write on Table2
        assert manager.has_permission("user1-token", "dbo.Table2", PermissionAccess.WRITE)

        # User1 has only read on Table1 (not write)
        assert not manager.has_permission("user1-token", "dbo.Table1", PermissionAccess.WRITE)

        # User1 has read_write on Table3 (includes write)
        assert manager.has_permission("user1-token", "dbo.Table3", PermissionAccess.WRITE)

    def test_has_read_write_permission(self, manager: PermissionManager):
        """Test read_write permission checks."""
        # User1 has explicit read_write on Table3
        assert manager.has_permission("user1-token", "dbo.Table3", PermissionAccess.READ_WRITE)

        # User1 has only read on Table1
        assert not manager.has_permission("user1-token", "dbo.Table1", PermissionAccess.READ_WRITE)

        # User2 has read_write on Table1
        assert manager.has_permission("user2-token", "dbo.Table1", PermissionAccess.READ_WRITE)

    def test_no_permission_for_unlisted_table(self, manager: PermissionManager):
        """Test that unlisted tables have no permission."""
        assert not manager.has_permission("user1-token", "dbo.UnknownTable", PermissionAccess.READ)

    def test_no_permission_for_unknown_user(self, manager: PermissionManager):
        """Test that unknown users have no permission."""
        assert not manager.has_permission("unknown-token", "dbo.Table1", PermissionAccess.READ)

    def test_get_table_permission(self, manager: PermissionManager):
        """Test getting table permission details."""
        perm = manager.get_table_permission("user1-token", "dbo.Table3")
        assert perm is not None
        assert perm.access == PermissionAccess.READ_WRITE
        assert perm.max_delete_rows == 1000

        perm = manager.get_table_permission("user1-token", "dbo.Unknown")
        assert perm is None

    def test_check_read_permission(self, manager: PermissionManager):
        """Test check_read_permission raises on denial."""
        # Should not raise
        manager.check_read_permission("user1-token", "dbo.Table1")

        # Should raise
        with pytest.raises(PermissionDeniedError) as exc_info:
            manager.check_read_permission("user1-token", "dbo.Unknown")

        assert exc_info.value.table == "dbo.Unknown"
        assert exc_info.value.operation == "read"

    def test_check_write_permission(self, manager: PermissionManager):
        """Test check_write_permission raises on denial."""
        # Should not raise
        manager.check_write_permission("user1-token", "dbo.Table2")

        # Should raise
        with pytest.raises(PermissionDeniedError) as exc_info:
            manager.check_write_permission("user1-token", "dbo.Table1")

        assert exc_info.value.operation == "write"

    def test_check_delete_limit(self, manager: PermissionManager):
        """Test delete limit enforcement."""
        # Within limit - should not raise
        manager.check_delete_limit("user1-token", "dbo.Table3", 500)

        # At limit - should not raise
        manager.check_delete_limit("user1-token", "dbo.Table3", 1000)

        # Over limit - should raise
        with pytest.raises(PermissionDeniedError) as exc_info:
            manager.check_delete_limit("user1-token", "dbo.Table3", 1001)

        assert "1001 > 1000" in str(exc_info.value)

    def test_check_delete_limit_no_permission(self, manager: PermissionManager):
        """Test delete limit check fails for unpermitted table."""
        with pytest.raises(PermissionDeniedError):
            manager.check_delete_limit("user1-token", "dbo.Unknown", 1)

    def test_list_accessible_tables(self, manager: PermissionManager):
        """Test listing accessible tables."""
        tables = manager.list_accessible_tables("user1-token")
        assert set(tables) == {"dbo.Table1", "dbo.Table2", "dbo.Table3"}

        # Filter by access
        tables = manager.list_accessible_tables("user1-token", PermissionAccess.READ)
        assert set(tables) == {"dbo.Table1", "dbo.Table3"}

        tables = manager.list_accessible_tables("user1-token", PermissionAccess.WRITE)
        assert set(tables) == {"dbo.Table2", "dbo.Table3"}

    def test_empty_manager(self):
        """Test manager with no users."""
        manager = PermissionManager([])

        assert manager.get_user("any-token") is None
        assert not manager.has_permission("any-token", "any-table", PermissionAccess.READ)
        assert manager.list_accessible_tables("any-token") == []
