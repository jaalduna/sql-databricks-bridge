"""Terminal UI for monitoring SQL-Databricks sync jobs.

Reads the local SQLite job store and Phase 2 sync queue directly to provide
a live view of ongoing and recent extractions, with drill-down to failures.

Also exposes an Events screen (press ``e``) that pulls the Databricks
``bridge_events`` table on demand — useful for verifying reverse-sync runs.
Pull is always manual so we don't thrash the SQL Warehouse.
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from rich.markup import escape
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.css.query import NoMatches
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Static


def _fmt_duration(created: str | None, completed: str | None) -> str:
    if not created:
        return "-"
    try:
        start = datetime.fromisoformat(created)
    except ValueError:
        return "-"
    try:
        end = datetime.fromisoformat(completed) if completed else datetime.utcnow()
    except ValueError:
        end = datetime.utcnow()
    secs = int((end - start).total_seconds())
    if secs < 0:
        return "-"
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m{secs % 60:02d}s"
    return f"{secs // 3600}h{(secs % 3600) // 60:02d}m"


def _summarize_error(err: str) -> str:
    """Reduce a raw error to a short, human-readable root cause.

    Matches common Databricks/SQL Server/parquet patterns; falls back to the
    first line stripped of SDK wrappers when no pattern matches.
    """
    if not err:
        return ""

    # Schema mismatch (Delta)
    if (
        "_LEGACY_ERROR_TEMP_DELTA_0007" in err
        or "schema mismatch detected" in err.lower()
    ):
        return "schema mismatch: delta table columns diverge from parquet"

    # Type cast mismatch — prefer column name if available
    m = re.search(r'cannot cast "([^"]+)" to "([^"]+)"', err)
    if m:
        col_m = re.search(r'resolve "([^"]+)"', err)
        col = f" on `{col_m.group(1)}`" if col_m else ""
        return f"type mismatch: {m.group(1)} → {m.group(2)}{col}"

    # Unresolved column / missing reference
    m = re.search(r'Cannot resolve "([^"]+)"', err)
    if m:
        return f"unresolved column: {m.group(1)}"
    m = re.search(r'column ["\']?([^"\',\s]+)["\']?.*?(?:does not exist|not found)', err, re.I)
    if m:
        return f"missing column: {m.group(1)}"

    # Timeout / network
    if re.search(r"\btimed? out\b|timeout expired", err, re.I):
        return "timeout"
    if re.search(r"connection refused|host is unreachable|network is unreachable", err, re.I):
        return "network: connection refused"

    # Auth / permissions
    if re.search(r"login failed|authentication failed|access denied|401|403|unauthorized", err, re.I):
        return "auth/permission denied"

    # Missing file (staged parquet likely cleaned up)
    if re.search(r"no such file|filenotfound|file.*not found|does not exist|404", err, re.I):
        return "missing staged parquet/file"

    # Table exists conflict
    if re.search(r"table .* already exists", err, re.I):
        return "table already exists"

    # Databricks ServiceError wrapper: extract inner message
    m = re.search(r"message=['\"](.+?)['\"](?=[\s,)])", err, re.S)
    core = m.group(1).strip() if m else err

    # Strip SQL state suffix ("SQLSTATE: XXXXX")
    core = re.sub(r"\s*SQLSTATE:\s*\S+\s*$", "", core)

    # Take first line
    first = core.splitlines()[0].strip()
    if len(first) > 120:
        first = first[:117] + "…"
    return first


def _clean_error_excerpt(err: str) -> str:
    """Strip SDK wrappers and return a readable 1-line excerpt of the raw error."""
    if not err:
        return ""
    m = re.search(r"message=['\"](.+?)['\"](?=[\s,)])", err, re.S)
    core = m.group(1) if m else err
    core = core.replace("\\n", " ").replace("\n", " ")
    core = re.sub(r"\s*SQLSTATE:\s*\S+.*$", "", core)
    core = re.sub(r"\s+", " ", core).strip()
    if len(core) > 220:
        core = core[:217] + "…"
    return core


def _fmt_secs(secs: float) -> str:
    """Format seconds: '12.3s' when < 60s, 'm:ss' or 'h:mm:ss' otherwise."""
    if secs < 60:
        return f"{secs:.1f}s"
    total = int(round(secs))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def _status_label(status: str) -> str:
    colors = {
        "running": "yellow",
        "completed": "green",
        "failed": "red",
        "pending": "blue",
        "cancelled": "magenta",
    }
    color = colors.get(status, "white")
    return f"[{color}]{status}[/{color}]"


def _load_jobs(db_path: str, limit: int, failed_only: bool) -> list[dict[str, Any]]:
    if not Path(db_path).exists():
        return []
    conn = sqlite3.connect(db_path, timeout=5)
    try:
        conn.row_factory = sqlite3.Row
        where = "WHERE status = 'failed'" if failed_only else ""
        rows = conn.execute(
            f"SELECT * FROM trigger_jobs {where} "
            "ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            for k in ("queries", "failed_queries", "results", "running_queries"):
                raw = d.get(k)
                try:
                    d[k] = json.loads(raw) if raw else []
                except (json.JSONDecodeError, TypeError):
                    d[k] = []
            result.append(d)
        return result
    finally:
        conn.close()


def _count_phase2_pending(db_path: str) -> dict[str, int]:
    if not Path(db_path).exists():
        return {}
    conn = sqlite3.connect(db_path, timeout=5)
    try:
        rows = conn.execute(
            "SELECT job_id, COUNT(*) AS n FROM sync_queue "
            "WHERE status = 'pending' GROUP BY job_id"
        ).fetchall()
        return {r[0]: r[1] for r in rows}
    finally:
        conn.close()


def _phase2_items_for_job(db_path: str, job_id: str) -> list[dict[str, Any]]:
    if not Path(db_path).exists():
        return []
    conn = sqlite3.connect(db_path, timeout=5)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT table_name, operation, status, error, created_at, committed_at "
            "FROM sync_queue WHERE job_id = ? ORDER BY id",
            (job_id,),
        ).fetchall()
        items = [dict(r) for r in rows]
        # Dedupe: if both `X` and `X_full` are present, drop the base one.
        names = {it["table_name"] for it in items}
        drop = {
            n for n in names
            if not n.endswith("_full") and (n + "_full") in names
        }
        return [it for it in items if it["table_name"] not in drop]
    finally:
        conn.close()


class JobDetailScreen(Screen):
    """Detail view for a single job."""

    BINDINGS = [
        Binding("escape,q", "app.pop_screen", "Back"),
        Binding("r", "refresh", "Refresh"),
        # Absorb main-screen keys so they don't stack/mis-fire here
        # and don't show up in the footer.
        Binding("d,enter,f", "noop", show=False, priority=True),
    ]

    def action_noop(self) -> None:
        pass

    def __init__(self, job_id: str, jobs_db: str, queue_db: str) -> None:
        super().__init__()
        self.job_id = job_id
        self.jobs_db = jobs_db
        self.queue_db = queue_db

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(id="meta")
        yield Static(id="running")
        yield Static(id="timings")
        yield Static(id="failed")
        yield Static(id="phase2")
        yield Footer()

    def on_mount(self) -> None:
        self.refresh_detail()
        self.set_interval(2.0, self.refresh_detail)

    def action_refresh(self) -> None:
        self.refresh_detail()

    def refresh_detail(self) -> None:
        try:
            meta_widget = self.query_one("#meta", Static)
            running_widget = self.query_one("#running", Static)
            timings_widget = self.query_one("#timings", Static)
            failed_widget = self.query_one("#failed", Static)
            phase2_widget = self.query_one("#phase2", Static)
        except NoMatches:
            return

        jobs = _load_jobs(self.jobs_db, limit=500, failed_only=False)
        job = next((j for j in jobs if j["job_id"] == self.job_id), None)
        if not job:
            meta_widget.update("[red]Job not found[/red]")
            return

        total = len(job["queries"])
        done = len(job["results"])
        failed = len(job["failed_queries"])
        running = job["running_queries"]
        dur = _fmt_duration(job.get("created_at"), job.get("completed_at"))

        meta_lines = [
            f"[bold]{job['country']}[/bold] / {job['stage']}   "
            f"status: {_status_label(job['status'])}   "
            f"progress: {done}/{total}   "
            f"failed: {failed}   "
            f"duration: {dur}",
            f"job_id: [dim]{job['job_id']}[/dim]   "
            f"triggered_by: {job.get('triggered_by', '?')}",
        ]
        if job.get("error"):
            err_short = _summarize_error(job["error"])
            err_excerpt = _clean_error_excerpt(job["error"])
            meta_lines.append(
                f"[red]error:[/red] [yellow]{escape(err_short or job['error'])}[/yellow]"
            )
            if err_excerpt and err_excerpt != err_short:
                meta_lines.append(f"   [dim]{escape(err_excerpt)}[/dim]")
        meta_widget.update("\n".join(meta_lines))

        if running:
            txt = "[bold yellow]Running queries[/bold yellow]\n" + "\n".join(
                f"  • {q}" for q in running
            )
        else:
            txt = "[dim]No queries currently running[/dim]"
        running_widget.update(txt)

        # Query timings: per-table sync time (desc, keep only _full when duplicated).
        results = job.get("results") or []
        timed = [
            r for r in results
            if isinstance(r, dict) and (r.get("duration_seconds") or 0) > 0
        ]
        # Dedupe by query_name prefix: if both `X` and `X_full` exist, keep `_full`.
        names = {r.get("query_name", "") for r in timed}
        drop_names = {
            n for n in names
            if not n.endswith("_full") and (n + "_full") in names
        }
        timed = [r for r in timed if r.get("query_name", "") not in drop_names]
        timed.sort(key=lambda r: r.get("duration_seconds", 0) or 0, reverse=True)
        if timed:
            total = sum((r.get("duration_seconds") or 0) for r in timed)
            # Compute wall-clock to contrast with the (parallelizable) sum.
            wall_secs: float | None = None
            try:
                start_raw = job.get("started_at") or job.get("created_at")
                end_raw = job.get("completed_at")
                if start_raw:
                    start_dt = datetime.fromisoformat(start_raw)
                    end_dt = (
                        datetime.fromisoformat(end_raw)
                        if end_raw else datetime.utcnow()
                    )
                    wall_secs = (end_dt - start_dt).total_seconds()
            except ValueError:
                wall_secs = None

            header = (
                f"[bold]Query timings[/bold]   "
                f"{len(timed)} queries · sum {_fmt_secs(total)}"
            )
            if wall_secs and wall_secs > 0:
                header += f" · wall {_fmt_secs(wall_secs)}"
                if total > 0:
                    header += f" · ~{total / wall_secs:.1f}× parallel"
            lines = [header]
            for r in timed[:20]:
                dur = r.get("duration_seconds") or 0
                rows = r.get("rows_extracted") or 0
                name = r.get("query_name", "?")
                stat = r.get("status", "")
                style = {
                    "completed": "green",
                    "failed": "red",
                    "running": "yellow",
                }.get(stat, "white")
                pct = (dur / total * 100) if total > 0 else 0
                lines.append(
                    f"  [{style}]{_fmt_secs(dur):>8s}[/{style}]  "
                    f"{pct:5.1f}%   "
                    f"{rows:>10,} rows   {escape(name)}"
                )
            if len(timed) > 20:
                lines.append(f"  [dim]… {len(timed) - 20} more[/dim]")
            timings_widget.update("\n".join(lines))
        else:
            timings_widget.update("[dim]No timing data yet[/dim]")

        if job["failed_queries"]:
            # Build a name→error map from results (failed_queries often stores
            # just names; the error detail lives in results[].error).
            err_by_name: dict[str, str] = {}
            for r in results:
                if isinstance(r, dict):
                    name = r.get("query_name") or ""
                    err = r.get("error") or ""
                    if name and err:
                        err_by_name[name] = err

            lines = ["[bold red]Failed queries[/bold red]"]
            for f in job["failed_queries"]:
                if isinstance(f, dict):
                    name = f.get("query") or f.get("name") or "?"
                    err = f.get("error", "") or err_by_name.get(name, "")
                else:
                    name = str(f)
                    err = err_by_name.get(name, "")
                summary = _summarize_error(err)
                excerpt = _clean_error_excerpt(err)
                if summary:
                    lines.append(
                        f"  • [red]{escape(name)}[/red]  "
                        f"[yellow]{escape(summary)}[/yellow]"
                    )
                else:
                    lines.append(f"  • [red]{escape(name)}[/red]")
                if excerpt and excerpt != summary:
                    lines.append(f"      [dim]{escape(excerpt)}[/dim]")
            failed_widget.update("\n".join(lines))
        elif job.get("error"):
            failed_widget.update(
                "[dim]No per-query failures — see job-level error above[/dim]"
            )
        else:
            failed_widget.update("[green]No failures[/green]")

        phase2 = _phase2_items_for_job(self.queue_db, self.job_id)
        if phase2:
            pending = sum(1 for p in phase2 if p["status"] == "pending")
            fails = sum(1 for p in phase2 if p["status"] == "failed")
            lines = [
                f"[bold]Phase 2 queue[/bold]   pending: {pending}   failed: {fails}"
            ]
            for p in phase2[-20:]:
                style = {
                    "pending": "blue",
                    "committed": "green",
                    "failed": "red",
                }.get(p["status"], "white")
                raw_err = p.get("error") or ""
                summary = _summarize_error(raw_err)
                excerpt = _clean_error_excerpt(raw_err)
                err_tag = f"  [yellow]{escape(summary)}[/yellow]" if summary else ""
                lines.append(
                    f"  [{style}]{p['status']:9s}[/{style}] "
                    f"{p['operation']:18s} {escape(p['table_name'])}{err_tag}"
                )
                if excerpt and excerpt != summary:
                    lines.append(f"      [dim]{escape(excerpt)}[/dim]")
            phase2_widget.update("\n".join(lines))
        else:
            phase2_widget.update("[dim]No Phase 2 queue items[/dim]")


class SyncMonitorApp(App):
    """Main monitoring TUI."""

    CSS = """
    #summary { height: 1; padding: 0 1; background: $boost; }
    #jobs_table { height: 1fr; }
    #meta { padding: 1 2; background: $boost; }
    #running, #timings, #failed, #phase2 { padding: 1 2; }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Refresh"),
        Binding("f", "toggle_failed", "Failed only"),
        Binding("d,enter", "open_detail", "Detail"),
        Binding("e", "open_events", "Events"),
    ]

    TITLE = "SQL↔Databricks Sync Monitor"

    def __init__(self, jobs_db: str, queue_db: str, interval: float) -> None:
        super().__init__()
        self.jobs_db = jobs_db
        self.queue_db = queue_db
        self.interval = max(0.5, interval)
        self.failed_only = False
        self._job_ids: list[str] = []
        self._row_keys: dict[str, Any] = {}  # job_id -> RowKey
        self._col_keys: list[Any] = []

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(id="summary")
        yield DataTable(id="jobs_table", cursor_type="row", zebra_stripes=True)
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#jobs_table", DataTable)
        self._col_keys = list(
            table.add_columns(
                "Activity ↓", "Country", "Stage", "Status",
                "Progress", "Running", "Failed", "Ph2", "Dur",
            )
        )
        self.refresh_data()
        self.set_interval(self.interval, self.refresh_data)

    @staticmethod
    def _last_activity(job: dict[str, Any]) -> str:
        """Most recent timestamp for the job: completed > started > created."""
        return (
            job.get("completed_at")
            or job.get("started_at")
            or job.get("created_at")
            or ""
        )

    @staticmethod
    def _fmt_ts(ts: str | None) -> str:
        """Render timestamp as 'MM-DD HH:MM:SS' so the date is visible."""
        if not ts:
            return "-"
        try:
            dt = datetime.fromisoformat(ts)
        except ValueError:
            return ts[:19]
        return dt.strftime("%m-%d %H:%M:%S")

    def _build_cells(
        self, job: dict[str, Any], phase2_counts: dict[str, int]
    ) -> list[str]:
        total_q = len(job["queries"])
        done = len(job["results"])
        failed_n = len(job["failed_queries"])
        # A job-level error with no per-query failures still counts as a failure.
        if failed_n == 0 and job.get("error"):
            failed_n = 1
        running_list = job["running_queries"]
        if running_list:
            disp = f"{len(running_list)}: {running_list[0]}"
        elif job["status"] == "running":
            disp = "starting…"
        else:
            disp = "-"
        if len(disp) > 28:
            disp = disp[:27] + "…"
        ph2 = phase2_counts.get(job["job_id"], 0)
        return [
            self._fmt_ts(self._last_activity(job)),
            job["country"],
            job["stage"],
            _status_label(job["status"]),
            f"{done}/{total_q}",
            disp,
            f"[red]{failed_n}[/red]" if failed_n else "0",
            str(ph2) if ph2 else "-",
            _fmt_duration(job.get("created_at"), job.get("completed_at")),
        ]

    def action_refresh(self) -> None:
        self.refresh_data()

    def action_toggle_failed(self) -> None:
        self.failed_only = not self.failed_only
        self.refresh_data()

    def action_open_detail(self) -> None:
        self._open_detail_for_cursor()

    def action_open_events(self) -> None:
        if isinstance(self.screen, EventsScreen):
            return
        self.push_screen(EventsScreen())

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Handle Enter on a DataTable row (Textual fires this event)."""
        self._open_detail_for_cursor()

    def _open_detail_for_cursor(self) -> None:
        # If a detail screen is already on top, don't stack another.
        if isinstance(self.screen, JobDetailScreen):
            return
        try:
            table = self.query_one("#jobs_table", DataTable)
        except NoMatches:
            return
        row = table.cursor_row
        if row is None or row < 0 or row >= len(self._job_ids):
            return
        job_id = self._job_ids[row]
        self.push_screen(JobDetailScreen(job_id, self.jobs_db, self.queue_db))

    def refresh_data(self) -> None:
        # Skip refresh while a child screen (e.g. JobDetailScreen) covers the
        # main view — our widgets aren't in the DOM of the active screen.
        try:
            summary_widget = self.query_one("#summary", Static)
            table = self.query_one("#jobs_table", DataTable)
        except NoMatches:
            return

        jobs = _load_jobs(self.jobs_db, limit=50, failed_only=self.failed_only)
        phase2_counts = _count_phase2_pending(self.queue_db)

        # Sort: active jobs first, then by most recent activity desc.
        # Stable sort lets us compose these in two passes.
        jobs.sort(key=self._last_activity, reverse=True)
        _prio = {"running": 0, "pending": 1}
        jobs.sort(key=lambda j: _prio.get(j["status"], 2))

        running = sum(1 for j in jobs if j["status"] == "running")
        pending = sum(1 for j in jobs if j["status"] == "pending")
        completed = sum(1 for j in jobs if j["status"] == "completed")
        failed = sum(1 for j in jobs if j["status"] == "failed")
        phase2_total = sum(phase2_counts.values())
        filt = "[red]failed-only[/red]" if self.failed_only else "all"
        summary = (
            f"[yellow]running {running}[/yellow]  "
            f"[blue]pending {pending}[/blue]  "
            f"[green]completed {completed}[/green]  "
            f"[red]failed {failed}[/red]  "
            f"[cyan]phase2 pending {phase2_total}[/cyan]   —   "
            f"filter: {filt}   sort: active first, then latest activity   "
            f"[dim]refresh {self.interval}s[/dim]"
        )
        summary_widget.update(summary)

        desired_ids = [j["job_id"] for j in jobs]

        # If the set/order of jobs changed, rebuild the table.
        # Otherwise update cells in-place — no flicker.
        if desired_ids != self._job_ids:
            prev_row = table.cursor_row
            table.clear()
            self._row_keys = {}
            for j in jobs:
                cells = self._build_cells(j, phase2_counts)
                rk = table.add_row(*cells, key=j["job_id"])
                self._row_keys[j["job_id"]] = rk
            self._job_ids = desired_ids
            if prev_row is not None and 0 <= prev_row < len(desired_ids):
                try:
                    table.move_cursor(row=prev_row)
                except Exception:
                    pass
        else:
            for j in jobs:
                rk = self._row_keys.get(j["job_id"])
                if rk is None:
                    continue
                cells = self._build_cells(j, phase2_counts)
                for ck, value in zip(self._col_keys, cells):
                    try:
                        table.update_cell(rk, ck, value)
                    except Exception:
                        pass


# ---------------------------------------------------------------------------
# Events screen — reverse-sync events (bridge_events) pulled on demand
# ---------------------------------------------------------------------------


def _pull_bridge_events(limit: int = 200) -> tuple[list[dict[str, Any]], str | None]:
    """Fetch recent bridge_events rows.  Returns (rows, error_or_None).

    Imports happen inside the function so the TUI still starts if the
    Databricks SDK is unavailable — the user just sees an error on pull.
    """
    try:
        from sql_databricks_bridge.core.config import get_settings
        from sql_databricks_bridge.db.databricks import DatabricksClient

        settings = get_settings()
        # Prefer the poller's warehouse when configured — same isolation.
        dbx_settings = settings.databricks
        if dbx_settings.poller_warehouse_id and (
            dbx_settings.poller_warehouse_id != dbx_settings.warehouse_id
        ):
            dbx_settings = dbx_settings.model_copy(
                update={"warehouse_id": dbx_settings.poller_warehouse_id}
            )
        client = DatabricksClient(settings=dbx_settings)
        rows = client.execute_sql(
            f"SELECT event_id, status, operation, source_table, target_table, "
            f"priority, rows_expected, rows_affected, discrepancy, warning, "
            f"error_message, created_at, processed_at "
            f"FROM {settings.events_table} "
            f"ORDER BY created_at DESC LIMIT {int(limit)}"
        )
        return rows, None
    except Exception as e:
        return [], str(e)


class EventDetailScreen(Screen):
    """Full detail for one bridge_events row (error_message, metadata, etc.)."""

    BINDINGS = [
        Binding("escape,q", "app.pop_screen", "Back"),
    ]

    def __init__(self, event: dict[str, Any]) -> None:
        super().__init__()
        self.event = event

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(id="ev_meta")
        yield Static(id="ev_error")
        yield Footer()

    def on_mount(self) -> None:
        e = self.event
        meta = [
            f"[bold]{escape(str(e.get('event_id', '?')))}[/bold]   "
            f"status: {_status_label(str(e.get('status') or 'pending'))}   "
            f"op: {escape(str(e.get('operation') or '?'))}",
            f"source: [cyan]{escape(str(e.get('source_table') or '-'))}[/cyan]   "
            f"target: [cyan]{escape(str(e.get('target_table') or '-'))}[/cyan]",
            f"created_at:   {escape(str(e.get('created_at') or '-'))}",
            f"processed_at: {escape(str(e.get('processed_at') or '-'))}",
            f"rows expected: {e.get('rows_expected') or '-'}   "
            f"rows affected: {e.get('rows_affected') or '-'}   "
            f"discrepancy: {e.get('discrepancy') or '-'}",
        ]
        if e.get("warning"):
            meta.append(f"[yellow]warning:[/yellow] {escape(str(e['warning']))}")
        self.query_one("#ev_meta", Static).update("\n".join(meta))

        err_raw = str(e.get("error_message") or "")
        if err_raw:
            summary = _summarize_error(err_raw)
            excerpt = _clean_error_excerpt(err_raw)
            blocks = [
                f"[red bold]error[/red bold]   [yellow]{escape(summary or '')}[/yellow]",
                f"[dim]{escape(excerpt)}[/dim]",
                "",
                f"[dim]raw (up to 2000 chars):[/dim]",
                escape(err_raw[:2000]),
            ]
            self.query_one("#ev_error", Static).update("\n".join(blocks))
        else:
            self.query_one("#ev_error", Static).update("[green]no error[/green]")


class EventsScreen(Screen):
    """Reverse-sync events viewer (bridge_events)."""

    CSS = """
    #ev_summary { height: 1; padding: 0 1; background: $boost; }
    #ev_pull_status { height: 1; padding: 0 1; }
    #ev_table { height: 1fr; }
    #ev_meta { padding: 1 2; background: $boost; }
    #ev_error { padding: 1 2; }
    """

    BINDINGS = [
        Binding("escape,q", "app.pop_screen", "Back"),
        Binding("r", "pull", "Pull"),
        Binding("f", "toggle_failed", "Failed only"),
        Binding("enter,d", "open_detail", "Detail"),
    ]

    TITLE = "Reverse-Sync Events"

    def __init__(self) -> None:
        super().__init__()
        self._events: list[dict[str, Any]] = []
        self._row_events: list[dict[str, Any]] = []
        self._failed_only = False
        self._last_pull: datetime | None = None
        self._last_error: str | None = None
        self._pulling = False

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(id="ev_summary")
        yield Static(id="ev_pull_status")
        yield DataTable(id="ev_table", cursor_type="row", zebra_stripes=True)
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#ev_table", DataTable)
        table.add_columns(
            "Status", "Op", "Source → Target",
            "Created", "Processed", "Rows", "Error",
        )
        self._refresh_summary()
        self._refresh_status()
        # No automatic pull — user presses 'r' explicitly.

    def action_pull(self) -> None:
        if self._pulling:
            return
        self._pulling = True
        self._last_error = None
        self._refresh_status()
        self.run_worker(self._do_pull, exclusive=True, thread=True)

    def _do_pull(self) -> None:
        rows, err = _pull_bridge_events(limit=200)
        self.app.call_from_thread(self._on_pull_done, rows, err)

    def _on_pull_done(
        self, rows: list[dict[str, Any]], err: str | None
    ) -> None:
        self._pulling = False
        self._last_pull = datetime.now()
        self._last_error = err
        if not err:
            self._events = rows
        self._rebuild_table()
        self._refresh_summary()
        self._refresh_status()

    def action_toggle_failed(self) -> None:
        self._failed_only = not self._failed_only
        self._rebuild_table()
        self._refresh_summary()

    def action_open_detail(self) -> None:
        try:
            table = self.query_one("#ev_table", DataTable)
        except NoMatches:
            return
        row = table.cursor_row
        if row is None or row < 0 or row >= len(self._row_events):
            return
        self.app.push_screen(EventDetailScreen(self._row_events[row]))

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        self.action_open_detail()

    def _filtered(self) -> list[dict[str, Any]]:
        if self._failed_only:
            return [e for e in self._events if str(e.get("status") or "") == "failed"]
        return self._events

    def _refresh_summary(self) -> None:
        try:
            widget = self.query_one("#ev_summary", Static)
        except NoMatches:
            return
        events = self._events
        counts = {s: 0 for s in ("pending", "processing", "completed", "failed")}
        for e in events:
            s = str(e.get("status") or "")
            if s in counts:
                counts[s] += 1
        widget.update(
            f"[blue]pending {counts['pending']}[/blue]  "
            f"[yellow]processing {counts['processing']}[/yellow]  "
            f"[green]completed {counts['completed']}[/green]  "
            f"[red]failed {counts['failed']}[/red]   "
            f"filter: {'[red]failed-only[/red]' if self._failed_only else 'all'}   "
            f"total loaded: {len(events)}"
        )

    def _refresh_status(self) -> None:
        try:
            widget = self.query_one("#ev_pull_status", Static)
        except NoMatches:
            return
        if self._pulling:
            msg = "[yellow]pulling from Databricks…[/yellow]"
        elif self._last_error:
            msg = f"[red]pull failed:[/red] [dim]{escape(self._last_error[:200])}[/dim]"
        elif self._last_pull is None:
            msg = "[dim]no data yet — press [bold]r[/bold] to pull from Databricks[/dim]"
        else:
            ts = self._last_pull.strftime("%H:%M:%S")
            msg = f"[dim]last pull: {ts}   press [bold]r[/bold] to refresh[/dim]"
        widget.update(msg)

    def _rebuild_table(self) -> None:
        try:
            table = self.query_one("#ev_table", DataTable)
        except NoMatches:
            return
        table.clear()
        rows = self._filtered()
        self._row_events = rows
        for e in rows:
            st = str(e.get("status") or "pending")
            src = str(e.get("source_table") or "")
            tgt = str(e.get("target_table") or "")
            arrow = f"{escape(src)} → {escape(tgt)}" if src or tgt else "-"
            created = _fmt_event_ts(e.get("created_at"))
            processed = _fmt_event_ts(e.get("processed_at"))
            rows_aff = e.get("rows_affected")
            rows_txt = f"{int(rows_aff):,}" if rows_aff is not None else "-"
            err = str(e.get("error_message") or "")
            err_short = _summarize_error(err) if err else ""
            err_cell = f"[red]{escape(err_short)}[/red]" if err_short else ""
            table.add_row(
                _status_label(st),
                escape(str(e.get("operation") or "-")),
                arrow,
                created,
                processed,
                rows_txt,
                err_cell,
            )


def _fmt_event_ts(ts: Any) -> str:
    """Format a bridge_events timestamp cell ('MM-DD HH:MM:SS')."""
    if ts is None or ts == "":
        return "-"
    raw = str(ts)
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return raw[:19]
    return dt.strftime("%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Wire EventsScreen into the main app: `e` binding
# ---------------------------------------------------------------------------


def run_monitor(jobs_db: str, queue_db: str, interval: float = 2.0) -> None:
    """Launch the monitor TUI."""
    app = SyncMonitorApp(jobs_db=jobs_db, queue_db=queue_db, interval=interval)
    app.run()
