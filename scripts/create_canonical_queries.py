#!/usr/bin/env python3
"""Create canonical extraction queries - one per table with explicit column lists.

Analyzes all existing queries to extract column lists per table,
then generates simplified ELT queries without transformations.
"""

import re
from pathlib import Path
from collections import defaultdict
from typing import Set, Dict, List


def extract_table_name(from_clause: str) -> str | None:
    """Extract table name from FROM clause."""
    # Remove schema prefix (dbo., {schema}., ps_latam., etc.)
    from_clause = from_clause.strip().lower()

    # Remove database prefix like bo_sinc.nac_ato or ps_latam.loc_psdata_compras
    if "." in from_clause:
        parts = from_clause.split(".")
        # Take last part (table name)
        table = parts[-1]
    else:
        table = from_clause

    # Remove table aliases (single letter after space)
    table = re.sub(r'\s+[a-z]$', '', table)
    table = re.sub(r'\s+\w+$', '', table)

    # Remove brackets
    table = table.replace('[', '').replace(']', '')

    # Remove WITH (NOLOCK) hints
    table = table.split('(')[0].strip()

    return table if table else None


def extract_columns_from_query(sql_content: str) -> Set[str]:
    """Extract column names from SELECT clause."""
    columns = set()

    # Find SELECT ... FROM section
    select_pattern = r'select\s+(.*?)\s+from'
    match = re.search(select_pattern, sql_content, re.IGNORECASE | re.DOTALL)

    if not match:
        return columns

    select_clause = match.group(1)

    # Split by comma, but be careful with nested functions
    parts = []
    current = []
    paren_depth = 0

    for char in select_clause + ',':
        if char == '(':
            paren_depth += 1
        elif char == ')':
            paren_depth -= 1
        elif char == ',' and paren_depth == 0:
            parts.append(''.join(current).strip())
            current = []
            continue
        current.append(char)

    for part in parts:
        if not part:
            continue

        # Skip aggregation functions (COUNT, MAX, MIN, SUM, AVG)
        if any(func in part.lower() for func in ['count(', 'max(', 'min(', 'sum(', 'avg(', 'string_agg(']):
            continue

        # Skip complex expressions with CASE, CAST, CONVERT, CONCAT, etc.
        if any(keyword in part.lower() for keyword in ['case', 'cast', 'convert', 'concat', 'substring', 'datepart']):
            continue

        # Remove table aliases (j., h., rg., etc.)
        part = re.sub(r'\w+\.', '', part)

        # Remove AS aliases
        if ' as ' in part.lower():
            part = part.lower().split(' as ')[0].strip()

        # Clean up
        col = part.strip()
        if col and not col.startswith('--') and not col.startswith('(') and col != '*':
            columns.add(col)

    return columns


def extract_table_and_columns(query_file: Path) -> Dict[str, Set[str]]:
    """Extract table name and columns from a query file."""
    content = query_file.read_text(encoding='utf-8')

    # Remove comments first
    content_no_comments = re.sub(r'--.*$', '', content, flags=re.MULTILINE)

    # Find FROM clause (first one only, avoid subqueries)
    from_pattern = r'\bfrom\s+([\w\.\[\]\{\}]+)'
    from_match = re.search(from_pattern, content_no_comments, re.IGNORECASE)

    if not from_match:
        return {}

    table_name = extract_table_name(from_match.group(1))
    if not table_name or len(table_name) <= 2:  # Skip invalid short names like "the", "pnc"
        return {}

    columns = extract_columns_from_query(content_no_comments)

    # Skip if no valid columns extracted
    if not columns:
        return {}

    return {table_name: columns}


def has_time_filter(columns: Set[str]) -> str | None:
    """Determine if table has time-based columns (fact table)."""
    time_cols = ['periodo', 'data_compra', 'ano', 'data_compra_utilizada']

    for col in columns:
        if col.lower() in time_cols:
            return col.lower()

    return None


def generate_canonical_query(table_name: str, columns: Set[str]) -> str:
    """Generate canonical extraction query for a table."""
    # Sort columns for consistency
    sorted_cols = sorted(columns)

    # Detect time filter column
    time_col = has_time_filter(columns)

    # Build query
    query_lines = [
        f"-- canonical extraction query for {table_name}",
        f"-- columns: {len(sorted_cols)}",
        "",
        "select"
    ]

    # Add columns (4 space indent)
    for i, col in enumerate(sorted_cols):
        suffix = "," if i < len(sorted_cols) - 1 else ""
        query_lines.append(f"    {col}{suffix}")

    query_lines.append(f"from {table_name}")

    # Add time filter if applicable
    if time_col:
        if time_col == 'periodo':
            query_lines.append("where periodo >= {start_period} and periodo <= {end_period}")
        elif time_col in ['data_compra', 'data_compra_utilizada']:
            query_lines.append(f"where {time_col} >= {{start_date}} and {time_col} <= {{end_date}}")
        elif time_col == 'ano':
            query_lines.append("where ano >= {start_year} and ano <= {end_year}")

    query_lines.append("")

    return "\n".join(query_lines)


def main():
    """Main entry point."""
    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    queries_dir = project_root / "queries" / "countries"

    # Collect all tables and their columns across all countries
    table_columns: Dict[str, Set[str]] = defaultdict(set)

    print("🔍 Analyzing existing queries...")

    for country_dir in queries_dir.iterdir():
        if not country_dir.is_dir():
            continue

        for query_file in country_dir.glob("*.sql"):
            result = extract_table_and_columns(query_file)
            for table, cols in result.items():
                table_columns[table].update(cols)
                print(f"  {country_dir.name}/{query_file.name} -> {table} ({len(cols)} columns)")

    print(f"\n📊 Found {len(table_columns)} unique tables\n")

    # Generate canonical queries
    canonical_dir = project_root / "queries" / "canonical"
    canonical_dir.mkdir(exist_ok=True)

    for table, columns in sorted(table_columns.items()):
        if len(columns) == 0:
            print(f"⚠️  Skipping {table} (no columns extracted)")
            continue

        query_content = generate_canonical_query(table, columns)
        output_file = canonical_dir / f"{table}.sql"
        output_file.write_text(query_content, encoding='utf-8')

        time_col = has_time_filter(columns)
        time_marker = " [FACT TABLE]" if time_col else ""
        print(f"✅ {table}.sql ({len(columns)} columns){time_marker}")

    print(f"\n✨ Generated {len(table_columns)} canonical queries in {canonical_dir}")


if __name__ == "__main__":
    main()
