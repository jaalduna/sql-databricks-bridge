"""Stage configuration loader and tag builder."""

from datetime import date
from pathlib import Path

import yaml

from sql_databricks_bridge.core.paths import get_config_file


def load_stages(override_path: str = "") -> list[dict[str, str]]:
    """Load stages from a YAML file.

    Args:
        override_path: Explicit file path. When empty, the bundled
                       ``stages.yaml`` from ``sql_databricks_bridge.data``
                       is used via ``importlib.resources``.

    Returns:
        List of stage dicts with 'code' and 'name' keys.

    Raises:
        FileNotFoundError: If the resolved file does not exist.
    """
    if override_path and Path(override_path).exists():
        config_path = Path(override_path)
    else:
        config_path = get_config_file("stages.yaml")

    if not config_path.exists():
        raise FileNotFoundError(f"Stages config not found: {config_path}")

    with open(config_path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    return [{"code": s["code"], "name": s["name"]} for s in data.get("stages", [])]


def build_tag(country: str, stage: str) -> str:
    """Compose a tag: {country}-{stage}-{YYYY-MM-DD}."""
    return f"{country}-{stage}-{date.today().isoformat()}"
