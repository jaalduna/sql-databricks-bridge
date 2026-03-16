"""CLI commands for SQL-Databricks Bridge."""

import json
import logging
import os
import shutil
import stat
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.progress import Progress, TextColumn, TimeElapsedColumn
from rich.table import Table

from sql_databricks_bridge import __version__
from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.core.delta_writer import DeltaTableWriter
from sql_databricks_bridge.core.extractor import Extractor, concat_chunks
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.sql_server import SQLServerClient

GITHUB_RELEASES_URL = (
    "https://api.github.com/repos/jaalduna/sql-databricks-bridge/releases/latest"
)

app = typer.Typer(
    name="sql-databricks-bridge",
    help="Bidirectional SQL Server ↔ Databricks data sync",
    add_completion=False,
)

console = Console()


def _cleanup_old_binary() -> None:
    """Remove leftover .old binary from a previous self-update (Windows rename trick)."""
    if not getattr(sys, "frozen", False):
        return
    old_path = Path(sys.executable).with_suffix(
        ".old.exe" if sys.platform == "win32" else ".old"
    )
    if old_path.exists():
        try:
            old_path.unlink()
        except OSError:
            pass


@app.callback()
def _main_callback() -> None:
    """Entry callback — runs before every command."""
    _cleanup_old_binary()


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
    queries_path: Annotated[
        str,
        typer.Option(
            "--queries-path",
            "-q",
            help="Path to directory containing SQL query files",
        ),
    ],
    country: Annotated[
        str,
        typer.Option(
            "--country",
            help="Country name for query resolution",
        ),
    ],
    destination: Annotated[
        str,
        typer.Option(
            "--destination",
            "-d",
            help="catalog.schema override for output tables (optional, uses config defaults)",
        ),
    ] = None,
    queries: Annotated[
        list[str],
        typer.Option(
            "--query",
            help="Specific queries to run (can specify multiple)",
        ),
    ] = None,
    chunk_size: Annotated[
        int,
        typer.Option(
            "--chunk-size",
            help="Rows per extraction chunk",
        ),
    ] = 100_000,
    limit: Annotated[
        int,
        typer.Option(
            "--limit",
            "-l",
            help="Limit rows per query (for testing). Wraps queries with SELECT TOP N",
        ),
    ] = None,
    lookback_months: Annotated[
        int,
        typer.Option(
            "--lookback-months",
            help="Number of months to look back for fact queries (default: 24)",
        ),
    ] = 24,
    overwrite: Annotated[
        bool,
        typer.Option(
            "--overwrite",
            help="Overwrite existing files",
        ),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Enable verbose output",
        ),
    ] = False,
) -> None:
    """Extract data from SQL Server to Databricks Delta tables.

    Example:
        sql-databricks-bridge extract \\
            --queries-path ./queries \\
            --country bolivia \\
            --lookback-months 24

        # Override catalog.schema:
        sql-databricks-bridge extract \\
            --queries-path ./queries \\
            --country bolivia \\
            --destination kpi_dev_01.bronze \\
            --lookback-months 12
    """
    setup_logging(verbose)

    try:

        # Parse catalog.schema from --destination if provided
        target_catalog: str | None = None
        target_schema: str | None = None
        if destination and "." in destination:
            parts = destination.split(".", 1)
            target_catalog = parts[0]
            target_schema = parts[1]

        # Initialize components
        sql_client = SQLServerClient(country=country)
        databricks_client = DatabricksClient()
        extractor = Extractor(queries_path, sql_client)
        writer = DeltaTableWriter(databricks_client)

        # Create job
        job = extractor.create_job(
            country=country,
            destination=destination or "",
            queries=queries if queries else None,
            chunk_size=chunk_size,
        )

        console.print(f"[bold green]Starting extraction job:[/bold green] {job.job_id}")
        console.print(f"  Country: {country}")
        console.print(f"  Queries: {len(job.queries)}")
        console.print(f"  Lookback: {lookback_months} months")
        console.print(
            f"  Target: Delta tables ({target_catalog or 'default catalog'}.{target_schema or 'default schema'})"
        )
        if limit:
            console.print(f"  [yellow]Row limit: {limit:,} rows per query (testing mode)[/yellow]")
        console.print()

        # Run extraction with progress
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            for query_name in job.queries:
                task = progress.add_task(f"Extracting {query_name}...", total=None)

                try:
                    # Check if table exists
                    table_name = writer.resolve_table_name(
                        query_name, country, target_catalog, target_schema
                    )
                    if not overwrite and writer.table_exists(table_name):
                        progress.update(
                            task,
                            description=f"[yellow]Skipped {query_name} (table {table_name} exists)[/yellow]",
                        )
                        continue

                    # Execute query
                    import polars as pl
                    from datetime import datetime as _dt

                    _dl_start = _dt.utcnow()
                    chunks = list(
                        extractor.execute_query(
                            query_name,
                            country,
                            chunk_size,
                            limit=limit,
                            lookback_months=lookback_months,
                        )
                    )
                    _dl_secs = (_dt.utcnow() - _dl_start).total_seconds()

                    if chunks:
                        combined = concat_chunks(chunks)
                        logging.getLogger(__name__).info(
                            f"SQL download complete: {query_name} - "
                            f"{len(combined):,} rows in {_dl_secs:.1f}s"
                        )
                        _ul_start = _dt.utcnow()
                        result = writer.write_dataframe(
                            combined,
                            query_name,
                            country,
                            catalog=target_catalog,
                            schema=target_schema,
                        )
                        _ul_secs = (_dt.utcnow() - _ul_start).total_seconds()
                        logging.getLogger(__name__).info(
                            f"Databricks upload complete: {query_name} - "
                            f"{_ul_secs:.1f}s"
                        )

                        progress.update(
                            task,
                            description=f"[green]OK {query_name}: {result.rows:,} rows in {_dl_secs:.1f}s dl + {_ul_secs:.1f}s ul -> {result.table_name}[/green]",
                        )
                    else:
                        progress.update(
                            task,
                            description=f"[yellow]OK {query_name}: 0 rows[/yellow]",
                        )

                except Exception as e:
                    progress.update(
                        task,
                        description=f"[red]FAIL {query_name}: {e}[/red]",
                    )

        console.print()
        console.print("[bold green]Extraction complete![/bold green]")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1)


@app.command()
def list_queries(
    queries_path: Annotated[
        str,
        typer.Option(
            "--queries-path",
            "-q",
            help="Path to directory containing SQL query files",
        ),
    ],
    country: Annotated[
        str,
        typer.Option(
            "--country",
            help="Country name",
        ),
    ],
) -> None:
    """List available SQL queries for a country."""
    try:
        from sql_databricks_bridge.core.country_query_loader import CountryAwareQueryLoader

        loader = CountryAwareQueryLoader(queries_path)
        queries = loader.list_queries(country)

        table = Table(title=f"Available Queries for {country}")
        table.add_column("Query Name", style="cyan")
        table.add_column("Source", style="green")

        for name in queries:
            source = loader.get_query_source(name, country)
            table.add_row(name, source)

        console.print(table)
        console.print(f"\n[bold]Total:[/bold] {len(queries)} queries")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1)


# Removed show_params command - no longer using YAML parameter configuration


@app.command()
def test_connection(
    country: Annotated[
        str,
        typer.Option(
            "--country",
            help="Country code to test SQL Server connection (optional)",
        ),
    ] = None,
) -> None:
    """Test connections to SQL Server and Databricks."""
    settings = get_settings()

    # Test SQL Server
    console.print("[bold]Testing SQL Server connection...[/bold]")

    if country:
        console.print(f"  Using country-specific connection: {country}")
        sql_client = SQLServerClient(country=country)
    else:
        console.print(f"  Using default connection from .env")
        sql_client = SQLServerClient()

    sql_ok = sql_client.test_connection()

    if sql_ok:
        if country:
            try:
                from kantar_db_handler.configs import get_country_params

                params = get_country_params(country)
                console.print(
                    f"  [green]OK Connected to {params['server']}/{params['database']}[/green]"
                )
            except:
                console.print(f"  [green]OK Connection successful[/green]")
        else:
            console.print(f"  [green]OK Connected to {settings.sql_server.host}[/green]")
    else:
        console.print(f"  [red]FAIL Failed to connect[/red]")

    sql_client.close()

    # Test Databricks
    console.print("[bold]Testing Databricks connection...[/bold]")
    databricks_client = DatabricksClient()
    db_ok = databricks_client.test_connection()
    if db_ok:
        console.print(f"  [green]OK Connected to {settings.databricks.host}[/green]")
    else:
        console.print(f"  [red]FAIL Failed to connect to {settings.databricks.host}[/red]")

    if not (sql_ok and db_ok):
        raise typer.Exit(code=1)


@app.command()
def list_countries() -> None:
    """List available countries from kantar_db_handler."""
    try:
        from kantar_db_handler.configs import get_country_params
        import importlib.resources as pkg_resources

        # Get list of country config files
        try:
            config_files = pkg_resources.files("kantar_db_handler.config_files")
            countries = [
                f.name.replace(".json", "") for f in config_files.iterdir() if f.suffix == ".json"
            ]
        except Exception:
            console.print("[yellow]Could not list country files automatically[/yellow]")
            console.print(
                "Available countries (known): Argentina, Bolivia, Brasil, CAM, Chile, Colombia, Ecuador, Mexico, Peru"
            )
            return

        if not countries:
            console.print("[yellow]No country configurations found[/yellow]")
            return

        table = Table(title="Available Countries")
        table.add_column("Country", style="cyan")
        table.add_column("Server", style="green")
        table.add_column("Database", style="yellow")

        for country in sorted(countries):
            try:
                params = get_country_params(country)
                table.add_row(country, params.get("server", "-"), params.get("database", "-"))
            except Exception as e:
                table.add_row(country, "[red]Error loading config[/red]", str(e)[:30])

        console.print(table)

    except ImportError:
        console.print("[bold red]Error:[/bold red] kantar_db_handler not installed")
        console.print("Install it with: pip install kantar-db-handler")
        raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
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

    if getattr(sys, "frozen", False):
        # Compiled mode: use direct app import (string-based import won't work)
        from sql_databricks_bridge.main import app as fastapi_app

        if reload:
            console.print("[yellow]Warning: --reload is not supported in compiled mode[/yellow]")
        config = uvicorn.Config(app=fastapi_app, host=host, port=port, log_level="info")
        server = uvicorn.Server(config)
        server.run()
    else:
        uvicorn.run(
            "sql_databricks_bridge.main:app",
            host=host,
            port=port,
            reload=reload,
        )


def _parse_version(tag: str) -> tuple[int, ...]:
    """Parse a version tag like 'v0.1.2' into a comparable tuple."""
    return tuple(int(x) for x in tag.lstrip("v").split("."))


def _fetch_latest_release() -> dict:
    """Fetch the latest release metadata from GitHub.

    Tries `gh` CLI first (handles private repos automatically), then falls
    back to urllib with an optional GITHUB_TOKEN env var.
    """
    # 1. Try gh CLI (works for private repos without extra config)
    try:
        import subprocess

        result = subprocess.run(
            ["gh", "api", "repos/jaalduna/sql-databricks-bridge/releases/latest"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    # 2. Fall back to urllib (public repos or with GITHUB_TOKEN)
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "sql-databricks-bridge",
    }
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = urllib.request.Request(GITHUB_RELEASES_URL, headers=headers)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def _find_asset(assets: list[dict], platform: str) -> dict | None:
    """Find the correct binary asset for the given platform."""
    if platform == "win32":
        target_name = "sql-databricks-bridge.exe"
    else:
        target_name = "sql-databricks-bridge"

    for asset in assets:
        if asset["name"] == target_name:
            return asset
    return None


def _download_asset(url: str, dest: Path) -> None:
    """Download a release asset to a local path.

    Uses `gh` CLI when available (handles private repo auth), otherwise
    falls back to urllib with optional GITHUB_TOKEN.
    """
    # 1. Try gh CLI for download (handles private repos)
    try:
        import subprocess

        result = subprocess.run(
            ["gh", "api", url, "--header", "Accept: application/octet-stream", "--output", str(dest)],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode == 0 and dest.stat().st_size > 0:
            return
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        pass

    # 2. Fall back to urllib
    headers = {
        "Accept": "application/octet-stream",
        "User-Agent": "sql-databricks-bridge",
    }
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=300) as resp:
        with open(dest, "wb") as f:
            shutil.copyfileobj(resp, f)


@app.command(hidden=True, deprecated=True)
def self_update(
    check: Annotated[
        bool,
        typer.Option("--check", help="Only check for updates, don't download"),
    ] = False,
    force: Annotated[
        bool,
        typer.Option("--force", help="Update even if already on the latest version"),
    ] = False,
) -> None:
    """Check for updates and replace the running binary with the latest release."""
    console.print(
        "[bold red]self-update is disabled.[/bold red] "
        "Please update manually by replacing the executable."
    )
    raise typer.Exit(code=1)

    # 1. Fetch latest release
    try:
        release = _fetch_latest_release()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            console.print(
                "[bold red]Error:[/bold red] Could not access releases (HTTP 404).\n\n"
                "This is a private repository. To use self-update you need one of:\n"
                "  1. Install the [bold]gh[/bold] CLI and authenticate: [cyan]gh auth login[/cyan]\n"
                "  2. Set [bold]GITHUB_TOKEN[/bold] env var with a personal access token (repo scope)"
            )
        else:
            console.print(f"[bold red]Error fetching latest release:[/bold red] {e}")
        raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[bold red]Error fetching latest release:[/bold red] {e}")
        raise typer.Exit(code=1)

    tag = release["tag_name"]
    console.print(f"[bold]Latest release:[/bold]  {tag}")

    # 2. Compare versions
    try:
        current = _parse_version(__version__)
        latest = _parse_version(tag)
    except ValueError:
        console.print(f"[yellow]Could not parse versions for comparison[/yellow]")
        if not force:
            raise typer.Exit(code=1)
        current, latest = (0,), (1,)  # force will proceed anyway

    if current >= latest and not force:
        console.print("[green]Already up to date.[/green]")
        return

    if check:
        console.print(f"[cyan]Update available: v{__version__} -> {tag}[/cyan]")
        return

    # 3. Verify we're running as a compiled binary
    if not getattr(sys, "frozen", False):
        console.print(
            "[yellow]self-update is only supported for compiled (PyInstaller) binaries.[/yellow]"
        )
        console.print("When running from source, use git pull or pip install instead.")
        raise typer.Exit(code=1)

    # 4. Find the matching asset
    asset = _find_asset(release.get("assets", []), sys.platform)
    if asset is None:
        console.print(f"[bold red]No binary asset found for platform {sys.platform!r}[/bold red]")
        raise typer.Exit(code=1)

    console.print(f"[bold]Downloading:[/bold] {asset['name']} ({asset['size'] / 1_048_576:.1f} MB)")

    # 5. Download to a temp file in the same directory (ensures same filesystem for rename)
    current_binary = Path(sys.executable)
    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=current_binary.parent,
        prefix=".update-",
        suffix=current_binary.suffix,
    )
    os.close(tmp_fd)
    tmp_path = Path(tmp_path)

    try:
        _download_asset(asset["url"], tmp_path)
    except Exception as e:
        tmp_path.unlink(missing_ok=True)
        console.print(f"[bold red]Download failed:[/bold red] {e}")
        raise typer.Exit(code=1)

    # 6. Replace the running binary
    try:
        if sys.platform == "win32":
            # Windows: can't delete a running exe, but can rename it
            old_path = current_binary.with_suffix(".old.exe")
            old_path.unlink(missing_ok=True)
            current_binary.rename(old_path)
            tmp_path.rename(current_binary)
        else:
            # Linux/macOS: atomic replace via rename
            tmp_path.chmod(tmp_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
            tmp_path.rename(current_binary)
    except Exception as e:
        tmp_path.unlink(missing_ok=True)
        console.print(f"[bold red]Failed to replace binary:[/bold red] {e}")
        raise typer.Exit(code=1)

    console.print(f"[bold green]Updated to {tag} — please restart.[/bold green]")


@app.command(name="diff-sync")
def diff_sync(
    country: Annotated[
        str,
        typer.Option("--country", "-c", help="Specific country (default: all)"),
    ] = "",
    interval: Annotated[
        int,
        typer.Option("--interval", "-i", help="Minutes between rounds (default: 15)"),
    ] = 15,
    once: Annotated[
        bool,
        typer.Option("--once", help="Run once then exit (no repeat loop)"),
    ] = False,
    api_url: Annotated[
        str,
        typer.Option("--api-url", help="API base URL"),
    ] = "http://127.0.0.1:8000/api/v1",
    lookback_months: Annotated[
        int,
        typer.Option("--lookback-months", help="Rolling lookback months"),
    ] = 24,
    stage: Annotated[
        str,
        typer.Option("--stage", help="Stage code"),
    ] = "sincronizacion",
    log_file: Annotated[
        str,
        typer.Option("--log-file", help="CSV output path"),
    ] = "diff_sync_validation.csv",
) -> None:
    """Run periodic differential sync for all (or one) country.

    Triggers diff sync via the API, polls until done, logs results to CSV.
    Requires the bridge API server to be running.

    Examples:

        bridge diff-sync                              # all countries, 15-min loop

        bridge diff-sync --country bolivia --once     # Bolivia, one-shot

        bridge diff-sync --api-url http://myworkspace.kantar.com:8000/api/v1
    """
    from sql_databricks_bridge.core.diff_sync_runner import (
        DIFF_TABLES,
        append_results_csv,
        discover_countries,
        init_csv,
        run_diff_sync_round,
    )

    # Resolve countries
    if country:
        countries = [country]
    else:
        console.print("[bold]Discovering countries from API...[/bold]")
        try:
            countries = discover_countries(api_url)
        except Exception as e:
            console.print(f"[bold red]Failed to discover countries:[/bold red] {e}")
            console.print(f"Is the API running at {api_url}?")
            raise typer.Exit(code=1)

    console.print(f"[bold]Diff-sync validation[/bold]")
    console.print(f"  Countries: {', '.join(countries)}")
    console.print(f"  Tables:    {', '.join(DIFF_TABLES)}")
    console.print(f"  Stage:     {stage}")
    console.print(f"  Lookback:  {lookback_months} months")
    console.print(f"  Interval:  {interval} min {'(once)' if once else '(loop)'}")
    console.print(f"  Log file:  {log_file}")
    console.print()

    # Check API reachable
    try:
        from sql_databricks_bridge.core.diff_sync_runner import api_get

        api_get(api_url, "/events?limit=1")
        console.print("[green]API is reachable.[/green]\n")
    except Exception as e:
        console.print(f"[bold red]API not reachable at {api_url}:[/bold red] {e}")
        raise typer.Exit(code=1)

    init_csv(log_file)

    # Stability tracking per (country, table)
    stability: dict[str, int] = {}
    all_runs: list[list] = []  # list of per-round country results (flattened)
    run_num = 0

    def _on_progress(cty: str, msg: str) -> None:
        console.print(f"  [{cty}] {msg}")

    try:
        while True:
            run_num += 1
            ts = datetime.now().isoformat(timespec="seconds")
            console.print(f"\n[bold]{'='*65}[/bold]")
            console.print(f"  [bold]Run #{run_num}[/bold]  --  {ts}")
            console.print(f"[bold]{'='*65}[/bold]")

            results = run_diff_sync_round(
                api_base=api_url,
                countries=countries,
                stage=stage,
                lookback_months=lookback_months,
                poll_interval=10,
                on_progress=_on_progress,
            )

            # Print per-country summary table
            summary = Table(title=f"Run #{run_num} Summary")
            summary.add_column("Country", style="cyan")
            summary.add_column("Table", style="white")
            summary.add_column("Status", style="green")
            summary.add_column("Rows", justify="right")
            summary.add_column("Time", justify="right")
            summary.add_column("Stable", justify="right")

            round_tables: list[dict] = []
            for cr in results:
                if cr.error and not cr.tables:
                    summary.add_row(cr.country, "", f"[red]{cr.status}[/red]", "", "", "")
                    continue
                for t in cr.tables:
                    key = f"{cr.country}:{t.table}"
                    if t.rows_extracted == 0 and t.status == "completed":
                        stability[key] = stability.get(key, 0) + 1
                    else:
                        stability[key] = 0
                    streak = stability.get(key, 0)
                    status_style = "green" if t.status == "completed" else "red"
                    summary.add_row(
                        cr.country,
                        t.table,
                        f"[{status_style}]{t.status}[/{status_style}]",
                        f"{t.rows_extracted:,}",
                        f"{t.duration_s:.1f}s",
                        f"{streak}x",
                    )
                    round_tables.append({
                        "country": cr.country,
                        "table": t.table,
                        "rows": t.rows_extracted,
                    })

            console.print(summary)

            # Highlight changes
            changed = [
                f"{r['country']}/{r['table']}"
                for r in round_tables
                if r["rows"] > 0
            ]
            if changed:
                console.print(f"  [yellow]Changes detected:[/yellow] {', '.join(changed)}")
            else:
                console.print(f"  [green]No changes detected (all tables stable)[/green]")

            # CSV
            append_results_csv(log_file, run_num, results)
            all_runs.append(results)

            if once:
                break

            console.print(
                f"\n  Next run in {interval} minutes (Ctrl+C to stop)..."
            )
            time.sleep(interval * 60)

    except KeyboardInterrupt:
        _print_final_summary(console, all_runs, stability, DIFF_TABLES, countries)
        console.print(f"\n  Results saved to: {log_file}")


def _print_final_summary(
    console: Console,
    all_runs: list,
    stability: dict[str, int],
    diff_tables: list[str],
    countries: list[str],
) -> None:
    """Print aggregate summary across all runs on Ctrl+C."""
    console.print(f"\n\n[bold]{'='*65}[/bold]")
    console.print(f"  [bold]FINAL SUMMARY[/bold]  ({len(all_runs)} rounds)")
    console.print(f"[bold]{'='*65}[/bold]")

    if not all_runs:
        console.print("  No completed rounds.")
        return

    # Aggregate per (country, table)
    stats: dict[str, dict] = {}
    for round_results in all_runs:
        for cr in round_results:
            for t in cr.tables:
                key = f"{cr.country}:{t.table}"
                if key not in stats:
                    stats[key] = {
                        "country": cr.country,
                        "table": t.table,
                        "total_rows": 0,
                        "changed_runs": 0,
                        "total_runs": 0,
                        "total_time": 0.0,
                        "errors": 0,
                    }
                stats[key]["total_runs"] += 1
                stats[key]["total_rows"] += t.rows_extracted
                stats[key]["total_time"] += t.duration_s
                if t.rows_extracted > 0:
                    stats[key]["changed_runs"] += 1
                if t.error:
                    stats[key]["errors"] += 1

    tbl = Table(title="Aggregate Stats")
    tbl.add_column("Country", style="cyan")
    tbl.add_column("Table", style="white")
    tbl.add_column("Runs", justify="right")
    tbl.add_column("Changed", justify="right")
    tbl.add_column("Tot Rows", justify="right")
    tbl.add_column("Avg Time", justify="right")
    tbl.add_column("Errs", justify="right")
    tbl.add_column("Streak", justify="right")

    for cty in countries:
        for table in diff_tables:
            key = f"{cty}:{table}"
            s = stats.get(key)
            if not s:
                continue
            avg_time = s["total_time"] / s["total_runs"] if s["total_runs"] else 0
            streak = stability.get(key, 0)
            tbl.add_row(
                cty, table,
                str(s["total_runs"]),
                str(s["changed_runs"]),
                f"{s['total_rows']:,}",
                f"{avg_time:.1f}s",
                str(s["errors"]),
                f"{streak}x",
            )

    console.print(tbl)


if __name__ == "__main__":
    app()
