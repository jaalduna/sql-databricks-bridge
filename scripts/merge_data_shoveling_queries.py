#!/usr/bin/env python3
"""Merge queries from data_shoveling project into sql-databricks-bridge.

Extracts column names from SELECT * queries and creates explicit column lists.
"""

import re
from pathlib import Path
from typing import Set


def normalize_table_name(from_clause: str) -> str:
    """Extract and normalize table name from FROM clause."""
    table = from_clause.strip().lower()

    # Remove (nolock) hints
    table = table.split('(')[0].strip()

    # Remove schema prefix
    if '.' in table:
        table = table.split('.')[-1]

    # Remove brackets
    table = table.replace('[', '').replace(']', '')

    return table


def extract_query_info(query_file: Path) -> dict:
    """Extract table name and check if it uses SELECT *."""
    content = query_file.read_text(encoding='utf-8')

    # Remove comments
    content_no_comments = re.sub(r'--.*$', '', content, flags=re.MULTILINE)

    # Find FROM clause
    from_match = re.search(r'\bfrom\s+([\w\.\[\]\(\)]+)', content_no_comments, re.IGNORECASE)
    if not from_match:
        return None

    table_name = normalize_table_name(from_match.group(1))

    # Check if it uses SELECT *
    uses_star = bool(re.search(r'\bselect\s+\*', content_no_comments, re.IGNORECASE))

    # Check for parameters
    has_params = bool(re.search(r'\{[\w_]+\}', content))

    return {
        'table_name': table_name,
        'uses_star': uses_star,
        'has_params': has_params,
        'original_content': content
    }


def main():
    """Main entry point."""
    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    # Source: data_shoveling queries
    source_queries = Path("../data_shoveling/src/data_shoveling/common/queries")

    # Target: our queries directory
    target_base = project_root / "queries"

    if not source_queries.exists():
        print(f"❌ Source not found: {source_queries}")
        return

    print("🔍 Analyzing data_shoveling queries...\n")

    # Analyze all queries
    new_tables = []
    existing_tables = []
    select_star_queries = []

    for query_file in sorted(source_queries.glob("*.sql")):
        info = extract_query_info(query_file)
        if not info:
            continue

        table_name = info['table_name']

        # Check if we already have this table in any country
        found_in_countries = []
        for country_dir in (target_base / "countries").iterdir():
            if not country_dir.is_dir():
                continue
            if (country_dir / f"{table_name}.sql").exists():
                found_in_countries.append(country_dir.name)

        if found_in_countries:
            existing_tables.append((table_name, found_in_countries))
        else:
            new_tables.append(table_name)

        if info['uses_star']:
            select_star_queries.append((query_file.stem, table_name))

    print("=" * 80)
    print("ANALYSIS RESULTS")
    print("=" * 80)

    print(f"\n✅ Tables we already have: {len(existing_tables)}")
    for table, countries in sorted(existing_tables):
        print(f"   {table}: in {len(countries)} countries")

    print(f"\n🆕 New tables from data_shoveling: {len(new_tables)}")
    for table in sorted(new_tables):
        print(f"   {table}")

    print(f"\n⚠️  Queries using SELECT * (need column inference): {len(select_star_queries)}")
    for query_name, table in sorted(select_star_queries):
        print(f"   {query_name}.sql → {table}")

    print("\n" + "=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)

    print("""
    The data_shoveling project uses SELECT * for many queries, which means
    we cannot automatically extract explicit column lists.

    Options:
    1. Keep SELECT * for these queries (not recommended for data governance)
    2. Query the actual SQL Server tables to get column lists (requires DB access)
    3. Manually specify column lists based on schema documentation
    4. Skip SELECT * queries and only merge queries with explicit columns

    New tables to consider adding:
    - fabricante (A_FabricanteProduto) - manufacturer dimension
    - marca (A_MarcaProduto) - brand dimension
    - produto (a_produto) - product dimension
    - conteudo (A_ConteudoProduto) - content dimension
    - sub (a_subproduto) - subproduct dimension
    - pais (pais) - country dimension
    - paneis_individuo (paneis_individuo) - panel individual
    - pesos_bebes (pesos_bebes) - baby weights
    - pets (pets) - pets dimension

    These are important dimension tables used across countries.
    """)


if __name__ == "__main__":
    main()
