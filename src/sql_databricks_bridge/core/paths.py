"""Path utilities using importlib.resources for bundled configs."""

from importlib.resources import files
from pathlib import Path


def get_config_file(name: str) -> Path:
    """Return the path to a bundled config file.

    Uses importlib.resources.files to locate YAML configs shipped inside
    the sql_databricks_bridge.data package.  Works in editable installs,
    standard pip installs, and PyInstaller/Nuitka compiled binaries.

    Args:
        name: Filename inside the data package (e.g. "stages.yaml").
    """
    resource = files("sql_databricks_bridge.data").joinpath(name)
    return Path(str(resource))
