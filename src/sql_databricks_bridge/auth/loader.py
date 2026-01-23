"""YAML permission file loader with hot reload support."""

import logging
import threading
from pathlib import Path
from typing import Callable

import yaml
from watchfiles import watch

from sql_databricks_bridge.api.schemas import (
    PermissionAccess,
    TablePermission,
    UserPermissions,
)
from sql_databricks_bridge.auth.permissions import PermissionManager

logger = logging.getLogger(__name__)


class PermissionLoader:
    """Loads permissions from YAML with optional hot reload."""

    def __init__(
        self,
        permissions_file: str | Path,
        hot_reload: bool = False,
        on_reload: Callable[[PermissionManager], None] | None = None,
    ) -> None:
        """Initialize permission loader.

        Args:
            permissions_file: Path to permissions YAML file.
            hot_reload: Enable file watching for hot reload.
            on_reload: Callback when permissions are reloaded.
        """
        self.permissions_file = Path(permissions_file)
        self.hot_reload = hot_reload
        self.on_reload = on_reload
        self._manager: PermissionManager | None = None
        self._watch_thread: threading.Thread | None = None
        self._stop_watch = threading.Event()

    def load(self) -> PermissionManager:
        """Load permissions from file.

        Returns:
            Permission manager with loaded permissions.

        Raises:
            FileNotFoundError: If file doesn't exist.
        """
        if not self.permissions_file.exists():
            raise FileNotFoundError(f"Permissions file not found: {self.permissions_file}")

        with open(self.permissions_file, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        users = self._parse_users(data.get("users", []))
        self._manager = PermissionManager(users)

        logger.info(f"Loaded {len(users)} users from {self.permissions_file}")

        # Start hot reload if enabled
        if self.hot_reload and self._watch_thread is None:
            self._start_watching()

        return self._manager

    def _parse_users(self, users_data: list[dict]) -> list[UserPermissions]:
        """Parse user data from YAML.

        Args:
            users_data: List of user dicts from YAML.

        Returns:
            List of UserPermissions objects.
        """
        users = []

        for user_data in users_data:
            permissions = []

            for perm_data in user_data.get("permissions", []):
                access_str = perm_data.get("access", "read")

                # Map string to enum
                access_map = {
                    "read": PermissionAccess.READ,
                    "write": PermissionAccess.WRITE,
                    "read_write": PermissionAccess.READ_WRITE,
                }

                permissions.append(
                    TablePermission(
                        table=perm_data["table"],
                        access=access_map.get(access_str, PermissionAccess.READ),
                        max_delete_rows=perm_data.get("max_delete_rows"),
                    )
                )

            users.append(
                UserPermissions(
                    token=user_data["token"],
                    name=user_data["name"],
                    permissions=permissions,
                )
            )

        return users

    def _start_watching(self) -> None:
        """Start file watching thread."""
        self._stop_watch.clear()
        self._watch_thread = threading.Thread(target=self._watch_file, daemon=True)
        self._watch_thread.start()
        logger.info(f"Started watching {self.permissions_file} for changes")

    def _watch_file(self) -> None:
        """Watch file for changes and reload."""
        for changes in watch(
            self.permissions_file.parent,
            stop_event=self._stop_watch,
        ):
            for change_type, path in changes:
                if Path(path) == self.permissions_file:
                    logger.info(f"Detected change in {self.permissions_file}, reloading...")
                    try:
                        new_manager = self.load()
                        if self.on_reload:
                            self.on_reload(new_manager)
                    except Exception as e:
                        logger.error(f"Failed to reload permissions: {e}")

    def stop_watching(self) -> None:
        """Stop file watching."""
        if self._watch_thread:
            self._stop_watch.set()
            self._watch_thread.join(timeout=5)
            self._watch_thread = None
            logger.info("Stopped watching permissions file")

    @property
    def manager(self) -> PermissionManager | None:
        """Get current permission manager."""
        return self._manager

    def reload(self) -> PermissionManager:
        """Force reload permissions.

        Returns:
            Reloaded permission manager.
        """
        return self.load()
