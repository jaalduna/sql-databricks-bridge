"""Entry point for running as module or compiled executable.

This module provides a standalone entry point for Nuitka compilation.
It can be run with: python -m sql_databricks_bridge [command]
Or as compiled executable: sql-databricks-bridge.exe [command]
"""

import sys


def main() -> None:
    """Main entry point."""
    from sql_databricks_bridge.cli.commands import app
    app()


if __name__ == "__main__":
    main()
