"""CLI commands for SQL-Databricks Bridge."""

import logging
import sys
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from sql_databricks_bridge import __version__
from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.core.extractor import Extractor
from sql_databricks_bridge.core.param_resolver import ParamResolver
from sql_databricks_bridge.core.query_loader import QueryLoader
from sql_databricks_bridge.core.uploader import Uploader
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.sql_server import SQLServerClient

app = typer.Typer(
    name="sql-databricks-bridge",
    help="Bidirectional SQL Server ↔ Databricks data sync",
    add_completion=False,
)

console = Console()


def setup_logging(verbose: bool) -> None:
    """Configure logging based on verbosity."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


@app.command()
def version() -> None:
    """Show version information."""
    console.print(f"sql-databricks-bridge v{__version__}")


@app.command()
def extract(
    queries_path: Annotated[str, typer.Option(
        "--queries-path", "-q",
        help="Path to directory containing SQL query files",
    )],
    config_path: Annotated[str, typer.Option(
        "--config-path", "-c",
        help="Path to directory containing YAML config files",
    )],
    country: Annotated[str, typer.Option(
        "--country",
        help="Country name for parameter resolution",
    )],
    destination: Annotated[str, typer.Option(
        "--destination", "-d",
        help="Databricks volume path for output files",
    )],
    queries: Annotated[list[str], typer.Option(
        "--query",
        help="Specific queries to run (can specify multiple)",
    )] = None,
    chunk_size: Annotated[int, typer.Option(
        "--chunk-size",
        help="Rows per extraction chunk",
    )] = 100_000,
    overwrite: Annotated[bool, typer.Option(
        "--overwrite",
        help="Overwrite existing files",
    )] = False,
    verbose: Annotated[bool, typer.Option(
        "--verbose", "-v",
        help="Enable verbose output",
    )] = False,
) -> None:
    """Extract data from SQL Server to Databricks.

    Example:
        sql-databricks-bridge extract \\
            --queries-path ./queries \\
            --config-path ./config \\
            --country Colombia \\
            --destination /Volumes/catalog/schema/volume/
    """
    setup_logging(verbose)

    try:
        # Initialize components
        sql_client = SQLServerClient()
        databricks_client = DatabricksClient()
        extractor = Extractor(queries_path, config_path, sql_client)
        uploader = Uploader(databricks_client)

        # Create job
        job = extractor.create_job(
            country=country,
            destination=destination,
            queries=queries if queries else None,
            chunk_size=chunk_size,
        )

        console.print(f"[bold green]Starting extraction job:[/bold green] {job.job_id}")
        console.print(f"  Country: {country}")
        console.print(f"  Queries: {len(job.queries)}")
        console.print(f"  Destination: {destination}")
        console.print()

        # Run extraction with progress
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            for query_name in job.queries:
                task = progress.add_task(f"Extracting {query_name}...", total=None)

                try:
                    # Check if file exists
                    output_path = f"{destination}/{country}/{query_name}/{query_name}.parquet"
                    if not overwrite and uploader.file_exists(output_path):
                        progress.update(task, description=f"[yellow]Skipped {query_name} (exists)[/yellow]")
                        continue

                    # Execute query
                    import polars as pl

                    chunks = list(extractor.execute_query(query_name, country, chunk_size))

                    if chunks:
                        combined = pl.concat(chunks)
                        results = uploader.upload_query_result(
                            combined,
                            destination,
                            query_name,
                            country,
                            chunk_size=chunk_size,
                        )

                        total_rows = sum(r.rows for r in results)
                        progress.update(
                            task,
                            description=f"[green]✓ {query_name}: {total_rows:,} rows[/green]",
                        )
                    else:
                        progress.update(
                            task,
                            description=f"[yellow]✓ {query_name}: 0 rows[/yellow]",
                        )

                except Exception as e:
                    progress.update(
                        task,
                        description=f"[red]✗ {query_name}: {e}[/red]",
                    )

        console.print()
        console.print("[bold green]Extraction complete![/bold green]")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1)


@app.command()
def list_queries(
    queries_path: Annotated[str, typer.Option(
        "--queries-path", "-q",
        help="Path to directory containing SQL query files",
    )],
) -> None:
    """List available SQL queries."""
    try:
        loader = QueryLoader(queries_path)
        queries = loader.discover_queries()

        table = Table(title="Available Queries")
        table.add_column("Query Name", style="cyan")
        table.add_column("Parameters", style="green")

        for name, sql in sorted(queries.items()):
            params = loader.get_query_parameters(sql)
            table.add_row(name, ", ".join(params) if params else "-")

        console.print(table)

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1)


@app.command()
def show_params(
    config_path: Annotated[str, typer.Option(
        "--config-path", "-c",
        help="Path to directory containing YAML config files",
    )],
    country: Annotated[str, typer.Option(
        "--country",
        help="Country name",
    )],
) -> None:
    """Show resolved parameters for a country."""
    try:
        resolver = ParamResolver(config_path)
        params = resolver.resolve_params(country)

        table = Table(title=f"Parameters for {country}")
        table.add_column("Parameter", style="cyan")
        table.add_column("Value", style="green")

        for key, value in sorted(params.items()):
            table.add_row(key, value)

        console.print(table)

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1)


@app.command()
def test_connection() -> None:
    """Test connections to SQL Server and Databricks."""
    settings = get_settings()

    # Test SQL Server
    console.print("[bold]Testing SQL Server connection...[/bold]")
    sql_client = SQLServerClient()
    sql_ok = sql_client.test_connection()

    if sql_ok:
        console.print(f"  [green]✓ Connected to {settings.sql_server.host}[/green]")
    else:
        console.print(f"  [red]✗ Failed to connect to {settings.sql_server.host}[/red]")

    sql_client.close()

    # Test Databricks
    console.print("[bold]Testing Databricks connection...[/bold]")
    databricks_client = DatabricksClient()
    db_ok = databricks_client.test_connection()

    if db_ok:
        console.print(f"  [green]✓ Connected to {settings.databricks.host}[/green]")
    else:
        console.print(f"  [red]✗ Failed to connect to {settings.databricks.host}[/red]")

    if not (sql_ok and db_ok):
        raise typer.Exit(code=1)


@app.command()
def serve(
    host: Annotated[str, typer.Option("--host", help="API host")] = "0.0.0.0",
    port: Annotated[int, typer.Option("--port", help="API port")] = 8000,
    reload: Annotated[bool, typer.Option("--reload", help="Enable auto-reload")] = False,
) -> None:
    """Start the API server."""
    import uvicorn

    console.print(f"[bold green]Starting API server on {host}:{port}[/bold green]")
    uvicorn.run(
        "sql_databricks_bridge.main:app",
        host=host,
        port=port,
        reload=reload,
    )


if __name__ == "__main__":
    app()
