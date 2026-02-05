"""Diagnose Bolivia extraction failures."""

from sql_databricks_bridge.db.sql_server import SQLServerClient
import traceback

print("=" * 80)
print("BOLIVIA EXTRACTION FAILURE DIAGNOSIS")
print("=" * 80)

client = SQLServerClient(country="bolivia")

failed_queries = [
    ("individuo", "Table exists but columns might be wrong"),
    ("loc_psdata_compras", "Table might not exist"),
    ("nac_ato", "Connection/timeout issue or column mismatch"),
    ("paineis", "Table exists but columns might be wrong"),
    ("pre_mordom", "Table exists but columns might be wrong"),
    ("ps_latam", "Table might not exist"),
    ("rg_domicilios_pesos", "Table exists but columns might be wrong"),
    ("rg_panelis", "Table exists but columns might be wrong"),
    ("sector", "Table might not exist (a_sector exists instead)"),
    ("tblproductosinternos", "Case sensitivity issue"),
    ("vw_artigoz", "View exists but columns might be wrong"),
    ("vw_venues", "View exists but columns might be wrong"),
]

print("\n1. CHECKING IF TABLES/VIEWS EXIST")
print("-" * 80)

for table_name, note in failed_queries:
    try:
        # Check if object exists
        query = f"""
        SELECT 
            OBJECT_ID('{table_name}') as obj_id,
            TYPE_NAME(OBJECT_ID('{table_name}')) as obj_type
        """
        result = client.execute_query(query)

        if result["obj_id"][0] is None:
            print(f"[NOT FOUND] {table_name:30s} - {note}")
        else:
            obj_type = result["obj_type"][0] if len(result) > 0 else "Unknown"
            print(f"[EXISTS]    {table_name:30s} - Type: {obj_type}")

    except Exception as e:
        print(f"[ERROR]     {table_name:30s} - {str(e)[:50]}")

print("\n2. CHECKING COLUMN COUNTS FOR EXISTING TABLES")
print("-" * 80)

for table_name, note in failed_queries:
    try:
        # Get column count
        query = f"""
        SELECT COUNT(*) as col_count
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
        """
        result = client.execute_query(query)

        if len(result) > 0 and result["col_count"][0] > 0:
            col_count = result["col_count"][0]
            print(f"{table_name:30s} - {col_count} columns found")

    except Exception as e:
        pass  # Already reported as not found

print("\n3. CHECKING SPECIFIC CRITICAL TABLES")
print("-" * 80)

# Check individuo
print("\n[individuo] - Checking columns...")
try:
    query = """
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'individuo'
    ORDER BY ORDINAL_POSITION
    """
    result = client.execute_query(query)
    if len(result) > 0:
        print(f"  Found {len(result)} columns:")
        for col in result["COLUMN_NAME"].to_list()[:10]:
            print(f"    - {col}")
        if len(result) > 10:
            print(f"    ... and {len(result) - 10} more")
except Exception as e:
    print(f"  ERROR: {e}")

# Check nac_ato
print("\n[nac_ato] - Checking if table exists and row count...")
try:
    query = "SELECT COUNT(*) as cnt FROM nac_ato WHERE 1=0"  # Quick check
    result = client.execute_query(query)
    print(f"  Table exists (checked with WHERE 1=0)")

    # Try to get actual count
    query = "SELECT COUNT(*) as cnt FROM nac_ato"
    result = client.execute_query(query)
    print(f"  Total rows: {result['cnt'][0]:,}")
except Exception as e:
    print(f"  ERROR: {type(e).__name__}: {str(e)[:100]}")

# Check j_atoscompra_new (empty table)
print("\n[j_atoscompra_new] - Why is it empty?")
try:
    query = "SELECT COUNT(*) as cnt FROM J_AtosCompra_New"
    result = client.execute_query(query)
    total = result["cnt"][0]
    print(f"  Total rows in table: {total:,}")

    if total > 0:
        query = """
        SELECT 
            MIN(dtcompra) as min_date,
            MAX(dtcompra) as max_date,
            COUNT(*) as cnt
        FROM J_AtosCompra_New
        """
        result = client.execute_query(query)
        print(f"  Date range: {result['min_date'][0]} to {result['max_date'][0]}")

        # Check 2025 data
        query = """
        SELECT COUNT(*) as cnt
        FROM J_AtosCompra_New
        WHERE YEAR(dtcompra) = 2025
        """
        result = client.execute_query(query)
        print(f"  Rows in 2025: {result['cnt'][0]:,}")

except Exception as e:
    print(f"  ERROR: {e}")

print("\n4. CHECKING VIEW COLUMN ISSUES")
print("-" * 80)

# Check vw_venues
print("\n[vw_venues] - Checking columns...")
try:
    query = """
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'vw_venues'
    ORDER BY ORDINAL_POSITION
    """
    result = client.execute_query(query)
    if len(result) > 0:
        cols = result["COLUMN_NAME"].to_list()
        print(f"  Found {len(result)} columns")

        # Check for problematic column
        if "format_group" in cols:
            print("  ✓ 'format_group' column EXISTS")
        else:
            print("  ✗ 'format_group' column MISSING")
            print(f"  Available columns: {', '.join(cols[:5])}...")
except Exception as e:
    print(f"  ERROR: {e}")

print("\n5. CASE SENSITIVITY CHECK")
print("-" * 80)

# Check tblproductosinternos with different cases
for variant in ["tblproductosinternos", "TblProductosInternos", "tblProductosInternos"]:
    try:
        query = f"SELECT COUNT(*) as cnt FROM {variant} WHERE 1=0"
        result = client.execute_query(query)
        print(f"  ✓ '{variant}' found")
        break
    except:
        print(f"  ✗ '{variant}' not found")

client.close()

print("\n" + "=" * 80)
print("DIAGNOSIS COMPLETE")
print("=" * 80)
