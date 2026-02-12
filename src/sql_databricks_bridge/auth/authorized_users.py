"""Authorized users allowlist loaded from YAML config."""

import logging
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class AuthorizedUser(BaseModel):
    """An authorized user from the allowlist."""

    email: str
    name: str
    roles: list[str] = Field(default_factory=lambda: ["operator"])
    countries: list[str] = Field(default_factory=list)

    @property
    def is_admin(self) -> bool:
        return "admin" in self.roles

    @property
    def can_trigger_sync(self) -> bool:
        """Viewers cannot trigger syncs."""
        return "viewer" not in self.roles or self.is_admin

    def can_access_country(self, country: str) -> bool:
        """Check if user is authorized for a specific country."""
        if self.is_admin:
            return True
        if "*" in self.countries:
            return True
        return country in self.countries


class AuthorizedUsersStore:
    """Loads and manages the authorized users allowlist from a YAML file."""

    def __init__(self, config_path: str | Path) -> None:
        self._config_path = Path(config_path)
        self._users: dict[str, AuthorizedUser] = {}

    def load(self) -> None:
        """Load authorized users from the YAML file.

        Raises:
            FileNotFoundError: If the config file does not exist.
        """
        if not self._config_path.exists():
            raise FileNotFoundError(
                f"Authorized users file not found: {self._config_path}"
            )

        with open(self._config_path, encoding="utf-8") as f:
            data: dict[str, Any] = yaml.safe_load(f) or {}

        self._users.clear()
        for user_data in data.get("users", []):
            user = AuthorizedUser(**user_data)
            self._users[user.email.lower()] = user

        logger.info(
            f"Loaded {len(self._users)} authorized users from {self._config_path}"
        )

    def get_user(self, email: str) -> AuthorizedUser | None:
        """Look up an authorized user by email (case-insensitive)."""
        return self._users.get(email.lower())

    @property
    def user_count(self) -> int:
        return len(self._users)
