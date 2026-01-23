"""Table-level permission management."""

import logging
from typing import Optional

from sql_databricks_bridge.api.schemas import (
    PermissionAccess,
    TablePermission,
    UserPermissions,
)

logger = logging.getLogger(__name__)


class PermissionDeniedError(Exception):
    """Raised when permission is denied for an operation."""

    def __init__(self, message: str, table: str | None = None, operation: str | None = None):
        super().__init__(message)
        self.table = table
        self.operation = operation


class PermissionManager:
    """Manages user permissions for table access."""

    def __init__(self, users: list[UserPermissions]) -> None:
        """Initialize permission manager.

        Args:
            users: List of user permissions.
        """
        self._users = {user.token: user for user in users}
        self._permission_cache: dict[str, dict[str, TablePermission]] = {}

        # Build permission cache
        for user in users:
            self._permission_cache[user.token] = {
                perm.table: perm for perm in user.permissions
            }

    def get_user(self, token: str) -> Optional[UserPermissions]:
        """Get user by token.

        Args:
            token: User token.

        Returns:
            User permissions if found.
        """
        return self._users.get(token)

    def has_permission(
        self,
        token: str,
        table: str,
        access: PermissionAccess,
    ) -> bool:
        """Check if user has permission for a table.

        Args:
            token: User token.
            table: Table name (e.g., "dbo.J_AtosCompra_New").
            access: Required access level.

        Returns:
            True if permission granted.
        """
        user_perms = self._permission_cache.get(token, {})
        table_perm = user_perms.get(table)

        if not table_perm:
            return False

        # Check access level
        if access == PermissionAccess.READ:
            return table_perm.access in (
                PermissionAccess.READ,
                PermissionAccess.READ_WRITE,
            )

        if access == PermissionAccess.WRITE:
            return table_perm.access in (
                PermissionAccess.WRITE,
                PermissionAccess.READ_WRITE,
            )

        if access == PermissionAccess.READ_WRITE:
            return table_perm.access == PermissionAccess.READ_WRITE

        return False

    def get_table_permission(
        self,
        token: str,
        table: str,
    ) -> Optional[TablePermission]:
        """Get permission details for a table.

        Args:
            token: User token.
            table: Table name.

        Returns:
            Table permission if exists.
        """
        user_perms = self._permission_cache.get(token, {})
        return user_perms.get(table)

    def check_read_permission(self, token: str, table: str) -> None:
        """Check read permission and raise if denied.

        Args:
            token: User token.
            table: Table name.

        Raises:
            PermissionDeniedError: If permission denied.
        """
        if not self.has_permission(token, table, PermissionAccess.READ):
            raise PermissionDeniedError(
                f"Read access denied for table: {table}",
                table=table,
                operation="read",
            )

    def check_write_permission(self, token: str, table: str) -> None:
        """Check write permission and raise if denied.

        Args:
            token: User token.
            table: Table name.

        Raises:
            PermissionDeniedError: If permission denied.
        """
        if not self.has_permission(token, table, PermissionAccess.WRITE):
            raise PermissionDeniedError(
                f"Write access denied for table: {table}",
                table=table,
                operation="write",
            )

    def check_delete_limit(
        self,
        token: str,
        table: str,
        row_count: int,
    ) -> None:
        """Check if delete operation is within allowed limit.

        Args:
            token: User token.
            table: Table name.
            row_count: Number of rows to delete.

        Raises:
            PermissionDeniedError: If row count exceeds limit.
        """
        perm = self.get_table_permission(token, table)

        if not perm:
            raise PermissionDeniedError(
                f"No permission for table: {table}",
                table=table,
                operation="delete",
            )

        if perm.max_delete_rows is not None and row_count > perm.max_delete_rows:
            raise PermissionDeniedError(
                f"Delete limit exceeded: {row_count} > {perm.max_delete_rows}",
                table=table,
                operation="delete",
            )

    def list_accessible_tables(
        self,
        token: str,
        access: PermissionAccess | None = None,
    ) -> list[str]:
        """List tables accessible by user.

        Args:
            token: User token.
            access: Filter by access level.

        Returns:
            List of accessible table names.
        """
        user_perms = self._permission_cache.get(token, {})

        if access is None:
            return list(user_perms.keys())

        return [
            table
            for table, perm in user_perms.items()
            if self.has_permission(token, table, access)
        ]
