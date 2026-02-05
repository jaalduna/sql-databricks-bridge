#!/usr/bin/env python3
"""Generate country-specific extraction queries based on actual column availability.

One query per table per country, with only columns available in that country.
"""

import re
from pathlib import Path
from collections import defaultdict
from typing import Set, Dict


def extract_table_name(from_clause: str) -> str | None:
    """Extract table name from FROM clause."""
    from_clause = from_clause.strip().lower()

    # Remove database prefix
    if "." in from_clause:
        parts = from_clause.split(".")
        table = parts[-1]
    else:
        table = from_clause

    # Remove table aliases
    table = re.sub(r'\s+\w+$', '', table)
    table = table.replace('[', '').replace(']', '')
    table = table.split('(')[0].strip()

    return table if table and len(table) > 2 else None


def extract_columns_from_query(sql_content: str) -> Set[str]:
    """Extract column names from SELECT clause."""
    columns = set()

    # Remove comments
    sql_content = re.sub(r'--.*$', '', sql_content, flags=re.MULTILINE)

    # Find SELECT ... FROM section
    select_pattern = r'select\s+(.*?)\s+from'
    match = re.search(select_pattern, sql_content, re.IGNORECASE | re.DOTALL)

    if not match:
        return columns

    select_clause = match.group(1)

    # Split by comma
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

        # Skip aggregations and complex expressions
        if any(func in part.lower() for func in ['count(', 'max(', 'min(', 'sum(', 'avg(', 'string_agg(', 'case', 'cast', 'convert', 'concat', 'substring', 'datepart', 'distinct']):
            continue

        # Remove table aliases
        part = re.sub(r'\w+\.', '', part)

        # Remove AS aliases
        if ' as ' in part.lower():
            part = part.lower().split(' as ')[0].strip()

        col = part.strip()
        if col and not col.startswith('--') and not col.startswith('(') and col != '*':
            columns.add(col)

    return columns


def has_time_filter(columns: Set[str]) -> str | None:
    """Determine if table has time-based columns (fact table)."""
    # Prioritize periodo over data_compra
    if 'periodo' in columns:
        return 'periodo'

    time_cols = ['data_compra', 'data_compra_utilizada', 'ano']
    for col in time_cols:
        if col in columns:
            return col

    return None


def generate_extraction_query(table_name: str, columns: Set[str], country: str) -> str:
    """Generate country-specific extraction query for a table."""
    # Sort columns for consistency
    sorted_cols = sorted(columns)

    # Detect time filter column
    time_col = has_time_filter(columns)

    # Build query
    query_lines = [
        f"-- {country}: {table_name}",
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

    print("🔍 Analyzing existing queries per country...\n")

    # Structure: {country: {table_name: set(columns)}}
    country_tables: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))

    # First pass: collect all tables and columns per country
    for country_dir in sorted(queries_dir.iterdir()):
        if not country_dir.is_dir():
            continue

        country = country_dir.name

        for query_file in country_dir.glob("*.sql"):
            content = query_file.read_text(encoding='utf-8')
            content_no_comments = re.sub(r'--.*$', '', content, flags=re.MULTILINE)

            # Find FROM clause
            from_pattern = r'\bfrom\s+([\w\.\[\]\{\}]+)'
            from_match = re.search(from_pattern, content_no_comments, re.IGNORECASE)

            if not from_match:
                continue

            table_name = extract_table_name(from_match.group(1))
            if not table_name:
                continue

            columns = extract_columns_from_query(content_no_comments)
            if columns:
                country_tables[country][table_name].update(columns)

    # Second pass: generate queries per country
    print("📝 Generating country-specific extraction queries...\n")

    total_queries = 0

    for country in sorted(country_tables.keys()):
        tables = country_tables[country]

        # Backup existing queries
        backup_dir = queries_dir / country / ".backup"
        backup_dir.mkdir(exist_ok=True)

        # Move existing queries to backup
        for existing_file in (queries_dir / country).glob("*.sql"):
            existing_file.rename(backup_dir / existing_file.name)

        print(f"📍 {country.upper()}")

        for table_name in sorted(tables.keys()):
            columns = tables[table_name]

            if len(columns) == 0:
                print(f"  ⚠️  {table_name}.sql (no columns, skipped)")
                continue

            query_content = generate_extraction_query(table_name, columns, country)
            output_file = queries_dir / country / f"{table_name}.sql"
            output_file.write_text(query_content, encoding='utf-8')

            time_col = has_time_filter(columns)
            time_marker = f" [FACT: {time_col}]" if time_col else ""
            print(f"  ✅ {table_name}.sql ({len(columns)} columns){time_marker}")
            total_queries += 1

        print(f"  🎉 {len(tables)} queries generated\n")

    print(f"✨ Total: {total_queries} country-specific queries generated")
    print(f"📦 Old queries backed up in each country's .backup/ directory")


if __name__ == "__main__":
    main()
