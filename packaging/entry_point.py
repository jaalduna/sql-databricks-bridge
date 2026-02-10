"""PyInstaller entry point for sql-databricks-bridge.

This script serves as the single entry point for the compiled executable.
It imports and runs the Typer CLI app which provides all commands:
  - extract: SQL Server -> Databricks extraction
  - serve: Start the API server
  - test-connection, list-queries, list-countries, version
"""

from sql_databricks_bridge.cli.commands import app

if __name__ == "__main__":
    app()
