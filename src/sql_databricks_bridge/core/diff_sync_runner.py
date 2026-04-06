"""Reusable diff-sync round logic shared by the CLI command and API endpoint.

Uses only ``urllib.request`` for HTTP so it works without the server internals
being importable (CLI, standalone scripts, compiled exe).
"""

from __future__ import annotations

import csv
import json
import logging
import os
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)

# The five fact tables that participate in differential sync.
DIFF_TABLES: list[str] = [
    "j_atoscompra_new",
    "vw_artigoz",
    "rg_domicilios_pesos",
    "domicilio_posse_bens",
    "mordom",
    "hato_cabecalho",
]

CSV_HEADERS = [
    "run",
    "timestamp",
    "country",
    "table",
    "status",
    "rows_extracted",
    "duration_s",
    "error",
]


# ---------------------------------------------------------------------------
# Result data classes
# ---------------------------------------------------------------------------


@dataclass
class TableResult:
    table: str
    status: str
    rows_extracted: int = 0
    duration_s: float = 0.0
    error: str = ""


@dataclass
class CountryRoundResult:
    country: str
    job_id: str = ""
    status: str = "pending"
    tables: list[TableResult] = field(default_factory=list)
    total_duration_s: float = 0.0
    error: str = ""


# ---------------------------------------------------------------------------
# HTTP helpers (stdlib only)
# ---------------------------------------------------------------------------


def api_post(api_base: str, path: str, data: dict) -> dict:
    body = json.dumps(data).encode()
    req = urllib.request.Request(
        f"{api_base}{path}",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def api_get(api_base: str, path: str) -> dict:
    req = urllib.request.Request(f"{api_base}{path}")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def discover_countries(api_base: str) -> list[str]:
    """Fetch the list of country codes from the metadata endpoint."""
    resp = api_get(api_base, "/metadata/countries")
    return [c["code"] for c in resp.get("countries", [])]


# ---------------------------------------------------------------------------
# Trigger + poll for a single country
# ---------------------------------------------------------------------------


def trigger_and_poll(
    api_base: str,
    country: str,
    stage: str = "sincronizacion",
    lookback_months: int = 24,
    poll_interval: int = 10,
    on_progress: Callable[[str, str], None] | None = None,
    table_suffix: str | None = None,
    all_tables: bool = False,
    suffix_exclude: list[str] | None = None,
) -> CountryRoundResult:
    """Trigger diff sync for *country*, poll until terminal, return result.

    Parameters
    ----------
    on_progress:
        Optional callback ``(country, message)`` called during polling for
        live output (CLI uses Rich, API ignores).
    all_tables:
        When True, pass ``queries=None`` to the trigger API so it discovers
        and syncs **all** available queries for the country (diff-sync tables
        use incremental mode, others use full extraction).
    suffix_exclude:
        Tables that should be synced WITHOUT the suffix (e.g. prediccion_compras).
    """
    t0 = time.time()

    # Trigger
    payload: dict = {
        "country": country,
        "stage": stage,
        "lookback_months": lookback_months,
        "force_full_sync": False,
    }
    if not all_tables:
        payload["queries"] = DIFF_TABLES
    # When all_tables=True, omit "queries" so the API discovers all of them
    if table_suffix:
        payload["table_suffix"] = table_suffix
    if suffix_exclude:
        payload["suffix_exclude"] = suffix_exclude
    try:
        resp = api_post(api_base, "/trigger", payload)
    except Exception as exc:
        return CountryRoundResult(
            country=country,
            status="failed",
            error=f"Trigger failed: {exc}",
            total_duration_s=time.time() - t0,
        )

    job_id = resp["job_id"]
    if on_progress:
        on_progress(country, f"Job {job_id} triggered")

    # Poll
    last_reported: set[str] = set()
    last_heartbeat = time.time()
    while True:
        time.sleep(poll_interval)
        try:
            detail = api_get(api_base, f"/events/{job_id}")
        except Exception as exc:
            if on_progress:
                on_progress(country, f"Poll error: {exc}")
            continue

        job_status = detail.get("status", "unknown")

        # Report per-table completions (only changes and failures)
        for r in detail.get("results", []):
            qn = r.get("query_name", "?")
            r_status = r.get("status", "?")
            if r_status in ("completed", "failed") and qn not in last_reported:
                rows = r.get("rows_extracted", 0)
                dur = r.get("duration_seconds", 0)
                err = r.get("error", "")
                last_reported.add(qn)
                # Only log tables with changes or failures
                if on_progress and (rows > 0 or r_status == "failed"):
                    marker = "OK" if r_status == "completed" else "FAIL"
                    msg = f"[{marker}] {qn}: {rows:,} rows in {dur:.1f}s"
                    if err:
                        msg += f" -- {err}"
                    on_progress(country, msg)

        if job_status in ("completed", "failed", "cancelled"):
            tables = _parse_results(detail)
            return CountryRoundResult(
                country=country,
                job_id=job_id,
                status=job_status,
                tables=tables,
                total_duration_s=time.time() - t0,
            )

        # Heartbeat — every 60s to reduce noise
        all_results = detail.get("results", [])
        completed_count = sum(
            1 for r in all_results
            if r.get("status") in ("completed", "failed")
        )
        total_count = len(all_results) or "?"
        now = time.time()
        if on_progress and (now - last_heartbeat) >= 60:
            elapsed = now - t0
            on_progress(
                country,
                f"{completed_count}/{total_count} done ({elapsed:.0f}s elapsed)",
            )
            last_heartbeat = now


def _parse_results(detail: dict) -> list[TableResult]:
    """Extract per-table results from an event detail response."""
    parsed: list[TableResult] = []
    for r in detail.get("results", []):
        parsed.append(
            TableResult(
                table=r.get("query_name", "unknown"),
                status=r.get("status", "unknown"),
                rows_extracted=r.get("rows_extracted", 0),
                duration_s=round(r.get("duration_seconds", 0), 1),
                error=r.get("error", ""),
            )
        )
    return parsed


# ---------------------------------------------------------------------------
# Full round (multiple countries, sequential)
# ---------------------------------------------------------------------------


def run_diff_sync_round(
    api_base: str,
    countries: list[str],
    stage: str = "sincronizacion",
    lookback_months: int = 24,
    poll_interval: int = 10,
    on_progress: Callable[[str, str], None] | None = None,
    table_suffix: str | None = None,
    all_tables: bool = False,
    suffix_exclude: list[str] | None = None,
) -> list[CountryRoundResult]:
    """Trigger diff sync for each country sequentially, poll until done.

    When *all_tables* is True, all available queries are synced (not just
    the 6 diff-sync tables).  Returns per-country results.
    """
    results: list[CountryRoundResult] = []
    for country in countries:
        if on_progress:
            on_progress(country, "Starting sync...")
        result = trigger_and_poll(
            api_base=api_base,
            country=country,
            stage=stage,
            lookback_months=lookback_months,
            poll_interval=poll_interval,
            on_progress=on_progress,
            table_suffix=table_suffix,
            all_tables=all_tables,
            suffix_exclude=suffix_exclude,
        )
        results.append(result)
    return results


# ---------------------------------------------------------------------------
# CSV logging
# ---------------------------------------------------------------------------


def init_csv(log_file: str) -> None:
    """Create CSV with headers if it doesn't already exist."""
    if not os.path.exists(log_file):
        with open(log_file, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(CSV_HEADERS)


def append_results_csv(
    log_file: str,
    run_num: int,
    results: list[CountryRoundResult],
) -> None:
    """Append per-table results from a round to the CSV log."""
    ts = datetime.now().isoformat(timespec="seconds")
    rows: list[list] = []
    for cr in results:
        if cr.tables:
            for t in cr.tables:
                rows.append([
                    run_num, ts, cr.country, t.table, t.status,
                    t.rows_extracted, t.duration_s, t.error,
                ])
        else:
            # No table results (e.g. trigger failure) — log the country error
            rows.append([
                run_num, ts, cr.country, "", cr.status, 0, 0, cr.error,
            ])

    with open(log_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)
