#!/usr/bin/env python3
"""Analyze column availability per table across countries."""

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

    return table if table else None


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
        if any(func in part.lower() for func in ['count(', 'max(', 'min(', 'sum(', 'avg(', 'case', 'cast', 'convert', 'concat', 'substring', 'datepart']):
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


def main():
    """Main entry point."""
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    queries_dir = project_root / "queries" / "countries"

    # Structure: {table_name: {country: set(columns)}}
    table_country_columns: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))

    print("🔍 Analyzing column availability by country...\n")

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
            if not table_name or len(table_name) <= 2:
                continue

            columns = extract_columns_from_query(content_no_comments)
            if columns:
                table_country_columns[table_name][country].update(columns)

    # Analyze each table
    print("=" * 80)
    print("TABLES WITH COUNTRY-SPECIFIC COLUMNS")
    print("=" * 80)

    for table in sorted(table_country_columns.keys()):
        countries_data = table_country_columns[table]

        # Get all unique columns across all countries
        all_columns = set()
        for cols in countries_data.values():
            all_columns.update(cols)

        # Find common columns (in all countries)
        countries_with_data = list(countries_data.keys())
        if len(countries_with_data) == 0:
            continue

        common_columns = set(countries_data[countries_with_data[0]])
        for country in countries_with_data[1:]:
            common_columns &= countries_data[country]

        # Find country-specific columns
        country_specific = {}
        for country, cols in countries_data.items():
            specific = cols - common_columns
            if specific:
                country_specific[country] = specific

        # Only print tables with differences
        if country_specific or len(countries_with_data) < 9:  # Not in all countries
            print(f"\n📊 {table.upper()}")
            print(f"   Countries: {', '.join(sorted(countries_with_data))}")
            print(f"   Common columns: {len(common_columns)}/{len(all_columns)}")

            if country_specific:
                print(f"   Country-specific columns:")
                for country in sorted(country_specific.keys()):
                    cols = sorted(country_specific[country])
                    print(f"      {country}: {', '.join(cols)}")

            if len(countries_with_data) < 9:
                missing = set(['argentina', 'bolivia', 'brazil', 'cam', 'chile', 'colombia', 'ecuador', 'mexico', 'peru']) - set(countries_with_data)
                if missing:
                    print(f"   ⚠️  Missing in: {', '.join(sorted(missing))}")

    print("\n" + "=" * 80)
    print("TABLES WITH UNIFORM COLUMNS (all countries have same columns)")
    print("=" * 80)

    for table in sorted(table_country_columns.keys()):
        countries_data = table_country_columns[table]
        countries_with_data = list(countries_data.keys())

        if len(countries_with_data) == 0:
            continue

        # Check if all countries have same columns
        common_columns = set(countries_data[countries_with_data[0]])
        all_same = True
        for country in countries_with_data[1:]:
            if countries_data[country] != common_columns:
                all_same = False
                break

        if all_same and len(countries_with_data) >= 2:
            print(f"   ✅ {table}: {len(common_columns)} columns in {len(countries_with_data)} countries")


if __name__ == "__main__":
    main()
