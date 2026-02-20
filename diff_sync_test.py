"""
2-level differential sync fingerprint test for Bolivia j_atoscompra_new (periods < 202501).
Runs entirely against on-premise SQL Server (KTCLSQL002/BO_KWP).

Level 1: GROUP BY periodo -> COUNT + CHECKSUM_AGG(CHECKSUM(*))
Level 2: GROUP BY idProduto for each period -> COUNT + CHECKSUM_AGG(CHECKSUM(*))

Output: fingerprints at both levels, showing data volume that would need
downloading if any period or product changed.
"""

import pyodbc
import time
import json

# --- Config ---
SQL_SERVER = "KTCLSQL002.KT.group.local"
SQL_DATABASE = "BO_KWP"
SQL_DRIVER = "ODBC Driver 17 for SQL Server"
SQL_USER = "luan.correa"
SQL_PASS = "K4nt4r0123"
TABLE = "j_atoscompra_new"
MAX_PERIOD = 202501


def get_conn():
    return pyodbc.connect(
        f"DRIVER={{{SQL_DRIVER}}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};PWD={SQL_PASS};TrustServerCertificate=yes;",
        timeout=120,
    )


def level1_period_fingerprints(conn):
    """Level 1: periodo -> count + checksum."""
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT periodo, COUNT(*) as cnt, CHECKSUM_AGG(CHECKSUM(*)) as chk
        FROM {TABLE}
        WHERE periodo < {MAX_PERIOD}
        GROUP BY periodo
        ORDER BY periodo
    """)
    return [(row[0], row[1], row[2]) for row in cursor.fetchall()]


def level2_product_fingerprints(conn, periodo):
    """Level 2: idProduto -> count + checksum for a specific period."""
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT idproduto, COUNT(*) as cnt, CHECKSUM_AGG(CHECKSUM(*)) as chk
        FROM {TABLE}
        WHERE periodo = {periodo}
        GROUP BY idproduto
        ORDER BY idproduto
    """)
    return [(row[0], row[1], row[2]) for row in cursor.fetchall()]


def main():
    print("=" * 70)
    print(f"2-LEVEL DIFFERENTIAL FINGERPRINT: Bolivia {TABLE} (periodo < {MAX_PERIOD})")
    print(f"Server: {SQL_SERVER} / {SQL_DATABASE}")
    print("=" * 70)

    conn = get_conn()
    print("Connected to SQL Server.\n")

    # === LEVEL 1 ===
    print("LEVEL 1: GROUP BY periodo -> COUNT + CHECKSUM_AGG(CHECKSUM(*))")
    print("-" * 70)
    t0 = time.time()
    periods = level1_period_fingerprints(conn)
    elapsed = time.time() - t0

    total_rows = sum(p[1] for p in periods)
    print(f"  Time: {elapsed:.1f}s | Periods: {len(periods)} | Total rows: {total_rows:,}\n")

    print(f"  {'periodo':<10} {'count':>12} {'checksum':>14}")
    print(f"  {'-'*10} {'-'*12} {'-'*14}")
    for periodo, cnt, chk in periods:
        print(f"  {periodo:<10} {cnt:>12,} {chk:>14,}")

    # === LEVEL 2 ===
    print("\n" + "=" * 70)
    print("LEVEL 2: For EACH period, GROUP BY idProduto -> COUNT + CHECKSUM_AGG(CHECKSUM(*))")
    print("=" * 70)

    all_products = {}
    total_l2_time = 0

    for periodo, period_cnt, period_chk in periods:
        t0 = time.time()
        products = level2_product_fingerprints(conn, periodo)
        elapsed = time.time() - t0
        total_l2_time += elapsed

        all_products[periodo] = products
        product_rows = sum(p[1] for p in products)

        print(f"\n  --- Period {periodo} ({period_cnt:,} rows, {len(products)} products, {elapsed:.1f}s) ---")
        # Show first 5 and last 2 products
        show = products[:5]
        if len(products) > 7:
            show += [("...", "...", "...")]
            show += products[-2:]
        elif len(products) > 5:
            show += products[5:]

        print(f"    {'idProduto':<12} {'count':>10} {'checksum':>14}")
        print(f"    {'-'*12} {'-'*10} {'-'*14}")
        for prod, cnt, chk in show:
            if prod == "...":
                print(f"    {'...':^38}")
            else:
                print(f"    {prod:<12} {cnt:>10,} {chk:>14,}")

    print(f"\n  Total Level 2 time: {total_l2_time:.1f}s")

    # === SUMMARY ===
    print("\n" + "=" * 70)
    print("DIFFERENTIAL SYNC IMPACT ANALYSIS")
    print("=" * 70)
    print(f"\nScenario: If Databricks already has all {len(periods)} periods cached,")
    print(f"and we detect N periods changed, here's the download volume:\n")

    print(f"  {'Changed periods':>18} {'Products to check':>20} {'Rows to download':>20} {'Savings vs full':>18}")
    print(f"  {'-'*18} {'-'*20} {'-'*20} {'-'*18}")

    for n_changed in [0, 1, 2, 5, 10, len(periods)]:
        if n_changed > len(periods):
            continue
        # Take the N largest periods as worst case
        sorted_periods = sorted(periods, key=lambda p: p[1], reverse=True)
        changed = sorted_periods[:n_changed]
        rows_to_check = sum(p[1] for p in changed)
        products_to_check = sum(len(all_products[p[0]]) for p in changed)
        savings = 100 * (1 - rows_to_check / total_rows) if total_rows else 0
        print(f"  {n_changed:>18} {products_to_check:>20,} {rows_to_check:>20,} {savings:>17.1f}%")

    # Show what Level 2 drill-down saves within a single period
    print(f"\nScenario: Within a changed period, if only M products changed:")
    # Use the largest period as example
    largest = max(periods, key=lambda p: p[1])
    lp_periodo, lp_cnt, _ = largest
    lp_products = all_products[lp_periodo]
    sorted_prods = sorted(lp_products, key=lambda p: p[1], reverse=True)

    print(f"  Example period: {lp_periodo} ({lp_cnt:,} rows, {len(lp_products)} products)\n")
    print(f"  {'Changed products':>18} {'Rows to download':>20} {'Savings vs period':>20}")
    print(f"  {'-'*18} {'-'*20} {'-'*20}")

    for n_prods in [0, 1, 2, 5, 10, len(lp_products)]:
        if n_prods > len(lp_products):
            continue
        changed_prods = sorted_prods[:n_prods]
        rows = sum(p[1] for p in changed_prods)
        savings = 100 * (1 - rows / lp_cnt) if lp_cnt else 0
        print(f"  {n_prods:>18} {rows:>20,} {savings:>19.1f}%")

    # Save fingerprints to JSON for later comparison with Databricks
    output = {
        "server": SQL_SERVER,
        "database": SQL_DATABASE,
        "table": TABLE,
        "max_period": MAX_PERIOD,
        "total_rows": total_rows,
        "level1": [
            {"periodo": p, "count": c, "checksum": k}
            for p, c, k in periods
        ],
        "level2": {
            str(periodo): [
                {"idproduto": p, "count": c, "checksum": k}
                for p, c, k in prods
            ]
            for periodo, prods in all_products.items()
        },
    }
    with open("diff_sync_fingerprints.json", "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nFingerprints saved to diff_sync_fingerprints.json")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
