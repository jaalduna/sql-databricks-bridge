"""Validate all SQL queries across all countries with SELECT TOP 1."""

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from dateutil.relativedelta import relativedelta

# Add project to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from sql_databricks_bridge.core.country_query_loader import CountryAwareQueryLoader
from sql_databricks_bridge.db.sql_server import SQLServerClient

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

QUERIES_PATH = Path(__file__).parent / "queries"
COUNTRIES = sorted(
    [d.name for d in (QUERIES_PATH / "countries").iterdir() if d.is_dir()]
)

# Placeholder substitution (same as extractor.py)
def substitute_placeholders(sql: str, lookback_months: int = 24) -> str:
    now = datetime.utcnow()
    start = now - relativedelta(months=lookback_months)
    sql = sql.replace("{lookback_months}", str(lookback_months))
    sql = sql.replace("{start_period}", start.strftime("%Y%m"))
    sql = sql.replace("{end_period}", now.strftime("%Y%m"))
    sql = sql.replace("{start_year}", str(start.year))
    sql = sql.replace("{end_year}", str(now.year))
    sql = sql.replace("{start_date}", f"'{start.strftime('%Y-%m-01')}'")
    sql = sql.replace("{end_date}", f"'{now.strftime('%Y-%m-%d')}'")
    return sql


def validate_query(client: SQLServerClient, query_name: str, sql: str) -> dict:
    """Run SELECT TOP 1 for a single query. Returns result dict."""
    sql = substitute_placeholders(sql)
    wrapped = f"SELECT TOP 1 * FROM ({sql}) AS _validate"
    t0 = time.time()
    try:
        from sqlalchemy import text
        with client.engine.connect() as conn:
            result = conn.execute(text(wrapped))
            cols = list(result.keys())
            row = result.fetchone()
            elapsed = time.time() - t0
            return {
                "query": query_name,
                "status": "OK",
                "columns": len(cols),
                "has_row": row is not None,
                "duration_s": round(elapsed, 2),
                "error": None,
            }
    except Exception as e:
        elapsed = time.time() - t0
        err_msg = str(e).split("\n")[0]  # first line only
        return {
            "query": query_name,
            "status": "FAIL",
            "columns": 0,
            "has_row": False,
            "duration_s": round(elapsed, 2),
            "error": err_msg,
        }


def validate_country(country: str, loader: CountryAwareQueryLoader) -> list[dict]:
    """Validate all queries for a given country."""
    print(f"\n{'='*60}")
    print(f"  {country.upper()}")
    print(f"{'='*60}")

    queries = loader.discover_queries(country)
    query_names = sorted(queries.keys())
    print(f"  Queries to validate: {len(query_names)}")

    # Create client for this country
    try:
        client = SQLServerClient(country=country)
        ok = client.test_connection()
        if not ok:
            print(f"  CONNECTION FAILED for {country}")
            return [
                {
                    "country": country,
                    "query": q,
                    "status": "FAIL",
                    "columns": 0,
                    "has_row": False,
                    "duration_s": 0,
                    "error": "Connection test failed",
                }
                for q in query_names
            ]
        print(f"  Connected to {client._server}/{client._database}")
    except Exception as e:
        print(f"  CONNECTION ERROR for {country}: {e}")
        return [
            {
                "country": country,
                "query": q,
                "status": "FAIL",
                "columns": 0,
                "has_row": False,
                "duration_s": 0,
                "error": f"Connection error: {e}",
            }
            for q in query_names
        ]

    results = []
    ok_count = 0
    fail_count = 0

    for i, qname in enumerate(query_names, 1):
        sql = queries[qname]
        r = validate_query(client, qname, sql)
        r["country"] = country
        results.append(r)

        status_icon = "OK" if r["status"] == "OK" else "FAIL"
        print(f"  [{i:3d}/{len(query_names)}] {status_icon} {qname} ({r['duration_s']}s)")
        if r["status"] == "FAIL":
            fail_count += 1
            print(f"           {r['error'][:120]}")
        else:
            ok_count += 1

    print(f"\n  Summary: {ok_count} OK, {fail_count} FAIL out of {len(query_names)}")
    client.close()
    return results


def main():
    # Accept country arguments from CLI
    target_countries = sys.argv[1:] if len(sys.argv) > 1 else COUNTRIES

    print("=" * 60)
    print("  SQL QUERY VALIDATION REPORT")
    print(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Countries: {', '.join(target_countries)}")
    print("=" * 60)

    loader = CountryAwareQueryLoader(QUERIES_PATH)

    all_results = []
    for country in target_countries:
        country_results = validate_country(country, loader)
        all_results.extend(country_results)
        # Clear cache for next country
        loader.reload()

    # ---- SUMMARY REPORT ----
    print("\n\n")
    print("=" * 80)
    print("  FINAL REPORT")
    print("=" * 80)

    total = len(all_results)
    ok_total = sum(1 for r in all_results if r["status"] == "OK")
    fail_total = sum(1 for r in all_results if r["status"] == "FAIL")

    print(f"\n  Total queries validated: {total}")
    print(f"  Passed: {ok_total}")
    print(f"  Failed: {fail_total}")

    # Per-country summary
    print(f"\n  {'Country':<15} {'Total':>6} {'OK':>6} {'FAIL':>6}")
    print(f"  {'-'*15} {'-'*6} {'-'*6} {'-'*6}")
    for country in target_countries:
        cr = [r for r in all_results if r["country"] == country]
        ok = sum(1 for r in cr if r["status"] == "OK")
        fail = sum(1 for r in cr if r["status"] == "FAIL")
        print(f"  {country:<15} {len(cr):>6} {ok:>6} {fail:>6}")

    # Failed queries detail
    failures = [r for r in all_results if r["status"] == "FAIL"]
    if failures:
        print(f"\n\n  FAILED QUERIES DETAIL ({len(failures)} total)")
        print(f"  {'='*75}")

        # Group by error pattern
        error_groups: dict[str, list[dict]] = {}
        for f in failures:
            # Normalize error to group similar ones
            err = f["error"] or "Unknown"
            # Extract the core error message
            if "Invalid column name" in err:
                key = "Invalid column name"
            elif "Invalid object name" in err:
                key = "Invalid object name"
            elif "does not exist" in err:
                key = "Object does not exist"
            elif "Connection" in err:
                key = "Connection error"
            elif "Conversion failed" in err:
                key = "Type conversion error"
            elif "Ambiguous column" in err:
                key = "Ambiguous column name"
            elif "permission" in err.lower() or "denied" in err.lower():
                key = "Permission denied"
            else:
                key = err[:80]
            error_groups.setdefault(key, []).append(f)

        for group_name, group_items in sorted(
            error_groups.items(), key=lambda x: -len(x[1])
        ):
            print(f"\n  [{len(group_items)}x] {group_name}")
            for item in group_items:
                print(f"    - {item['country']}/{item['query']}")
                if item["error"] != group_name:
                    print(f"      {item['error'][:120]}")
    else:
        print("\n  ALL QUERIES PASSED!")

    # Save JSON report (include country in filename if subset)
    suffix = f"_{'_'.join(target_countries)}" if len(target_countries) < len(COUNTRIES) else ""
    report_path = Path(__file__).parent / f"query_validation_report{suffix}.json"
    with open(report_path, "w") as fp:
        json.dump(all_results, fp, indent=2)
    print(f"\n  JSON report saved to: {report_path}")


if __name__ == "__main__":
    main()
