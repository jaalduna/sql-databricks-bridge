"""Stage configuration loader and tag builder."""

from datetime import date
from pathlib import Path

import yaml


def load_stages(path: str) -> list[dict[str, str]]:
    """Load stages from a YAML file.

    Args:
        path: Path to the stages YAML file.

    Returns:
        List of stage dicts with 'code' and 'name' keys.

    Raises:
        FileNotFoundError: If the YAML file does not exist.
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Stages config not found: {config_path}")

    with open(config_path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    return [{"code": s["code"], "name": s["name"]} for s in data.get("stages", [])]


def build_tag(country: str, stage: str) -> str:
    """Compose a tag: {country}-{stage}-{YYYY-MM-DD}."""
    return f"{country}-{stage}-{date.today().isoformat()}"
