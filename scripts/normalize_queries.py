#!/usr/bin/env python3
"""Normalize SQL queries: remove aliases and convert to lowercase."""

import re
from pathlib import Path


def remove_aliases_and_lowercase(sql: str) -> str:
    """Remove AS aliases and convert to lowercase.

    Transforms:
        idCidade AS region_id  ->  idcidade
        Data_Compra            ->  data_compra
        FROM dbo.Cidade        ->  FROM dbo.cidade
    """
    # First, remove all AS aliases (column AS alias_name)
    # Pattern: word/expression followed by AS followed by alias_name
    sql = re.sub(
        r'\b(\w+)\s+AS\s+\w+',
        r'\1',
        sql,
        flags=re.IGNORECASE
    )

    # Convert everything to lowercase
    sql = sql.lower()

    return sql


def normalize_queries_in_directory(directory: Path) -> None:
    """Normalize all .sql files in a directory."""
    if not directory.exists():
        print(f"  ⚠️  Directory not found: {directory}")
        return

    sql_files = list(directory.glob("*.sql"))
    if not sql_files:
        print(f"  ℹ️  No SQL files in {directory}")
        return

    for sql_file in sql_files:
        # Read original
        original_sql = sql_file.read_text(encoding="utf-8")

        # Normalize
        normalized_sql = remove_aliases_and_lowercase(original_sql)

        # Write back
        sql_file.write_text(normalized_sql, encoding="utf-8")

        print(f"  ✅ {sql_file.name}")


def main():
    """Normalize all queries in countries directories."""
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    queries_dir = project_root / "queries"

    print("🔧 Normalizing queries (remove aliases + lowercase)...\n")

    # Process each country
    countries_dir = queries_dir / "countries"

    if not countries_dir.exists():
        print(f"❌ Countries directory not found: {countries_dir}")
        return

    for country_dir in sorted(countries_dir.iterdir()):
        if country_dir.is_dir():
            print(f"📍 Processing {country_dir.name}...")
            normalize_queries_in_directory(country_dir)

    # Process common directory
    common_dir = queries_dir / "common"
    if common_dir.exists() and list(common_dir.glob("*.sql")):
        print(f"\n📍 Processing common...")
        normalize_queries_in_directory(common_dir)

    print("\n✨ Normalization complete!")


if __name__ == "__main__":
    main()
