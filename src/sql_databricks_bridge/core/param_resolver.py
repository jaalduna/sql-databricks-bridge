"""YAML parameter resolution and merging."""

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


class ConfigNotFoundError(Exception):
    """Raised when a configuration file is not found."""

    pass


def flatten_dict(d: dict[str, Any], parent_key: str = "", sep: str = ".") -> dict[str, str]:
    """Flatten nested dictionary into single-level dict with compound keys.

    Args:
        d: Nested dictionary to flatten.
        parent_key: Prefix for keys (used in recursion).
        sep: Separator between nested key levels (default "." for SQL param compatibility).

    Returns:
        Flattened dictionary with string values.

    Example:
        >>> flatten_dict({"a": {"b": 1, "c": 2}})
        {"a.b": "1", "a.c": "2"}
    """
    items: list[tuple[str, str]] = []

    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k

        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep).items())
        else:
            items.append((new_key, str(v) if v is not None else ""))

    return dict(items)


class ParamResolver:
    """Resolves and merges parameters from YAML configuration files."""

    def __init__(self, config_path: str | Path) -> None:
        """Initialize parameter resolver.

        Args:
            config_path: Path to directory containing YAML config files.
        """
        self.config_path = Path(config_path)
        self._common_params: dict[str, Any] | None = None

    def _load_yaml(self, filename: str) -> dict[str, Any]:
        """Load a YAML file from the config path.

        Args:
            filename: YAML filename to load.

        Returns:
            Parsed YAML content.

        Raises:
            ConfigNotFoundError: If file doesn't exist.
        """
        file_path = self.config_path / filename
        if not file_path.exists():
            raise ConfigNotFoundError(f"Config file not found: {file_path}")

        with open(file_path, encoding="utf-8") as f:
            content = yaml.safe_load(f) or {}

        logger.debug(f"Loaded config from {file_path}")
        return content

    def get_common_params(self) -> dict[str, Any]:
        """Get common parameters (cached).

        Returns:
            Common parameters dictionary.
        """
        if self._common_params is None:
            try:
                self._common_params = self._load_yaml("common_params.yaml")
            except ConfigNotFoundError:
                logger.warning("common_params.yaml not found, using empty defaults")
                self._common_params = {}

        return self._common_params

    def get_country_params(self, country: str) -> dict[str, Any]:
        """Get country-specific parameters.

        Args:
            country: Country name (e.g., "Colombia", "Brasil").

        Returns:
            Country-specific parameters dictionary.
        """
        try:
            return self._load_yaml(f"{country}.yaml")
        except ConfigNotFoundError:
            logger.warning(f"{country}.yaml not found, using empty defaults")
            return {}

    def resolve_params(self, country: str) -> dict[str, str]:
        """Resolve all parameters for a country.

        Merges common_params.yaml with {country}.yaml, with country
        params taking precedence. Result is flattened to single-level dict.

        Args:
            country: Country name.

        Returns:
            Flattened parameter dictionary with all values as strings.
        """
        common = self.get_common_params()
        country_cfg = self.get_country_params(country)

        # Deep merge: country params override common params
        merged = self._deep_merge(common, country_cfg)

        # Flatten to single-level dict with underscore separator for SQL compatibility
        flattened = flatten_dict(merged, sep="_")

        logger.info(f"Resolved {len(flattened)} parameters for {country}")
        return flattened

    def _deep_merge(self, base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
        """Deep merge two dictionaries, with override taking precedence.

        Args:
            base: Base dictionary.
            override: Override dictionary (takes precedence).

        Returns:
            Merged dictionary.
        """
        result = base.copy()

        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value

        return result

    def get_table_config(self, country: str, table_name: str) -> dict[str, str]:
        """Get table-specific configuration for a country.

        Args:
            country: Country name.
            table_name: Table name (query name).

        Returns:
            Table-specific parameters.
        """
        country_cfg = self.get_country_params(country)
        tables = country_cfg.get("tables", {})

        if table_name in tables:
            table_cfg = tables[table_name]
            if isinstance(table_cfg, dict):
                return flatten_dict(table_cfg)
            return {table_name: str(table_cfg)}

        return {}

    def reload(self) -> None:
        """Clear cached parameters to force reload."""
        self._common_params = None
