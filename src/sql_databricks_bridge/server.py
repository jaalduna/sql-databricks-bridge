"""Standalone server entry point for Nuitka compilation.

This module provides a direct entry point for running the API server
without relying on uvicorn's string-based module import, which doesn't
work with compiled executables.

Usage:
    python -m sql_databricks_bridge.server [--host HOST] [--port PORT]
    sql-databricks-bridge-server.exe [--host HOST] [--port PORT]
"""

import argparse
import sys

# Import at top-level so Nuitka can detect and include these modules
import uvicorn
from sql_databricks_bridge.main import app


def run_server(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Run the FastAPI server directly.

    Args:
        host: Host to bind to
        port: Port to listen on
    """
    # Use uvicorn's Server class directly for better Nuitka compatibility
    config = uvicorn.Config(
        app=app,
        host=host,
        port=port,
        log_level="info",
        access_log=True,
    )
    server = uvicorn.Server(config)
    server.run()


def main() -> None:
    """Parse arguments and run server."""
    parser = argparse.ArgumentParser(
        description="SQL-Databricks Bridge API Server",
        prog="sql-databricks-bridge-server",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind to (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to listen on (default: 8000)",
    )

    args = parser.parse_args()
    run_server(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
