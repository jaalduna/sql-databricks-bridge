#!/usr/bin/env python3
"""Merge KWP per-country folder queries into sql-databricks-bridge.

These queries are already resolved per country and just need normalization.
"""

import re
from pathlib import Path


# Country name mapping (source folder → target folder)
COUNTRY_MAPPING = {
    "bolivia": "bolivia",
    "brasil": "brazil",
    "cam": "cam",
    "chile": "chile",
    "colombia": "colombia",
    "ecuador": "ecuador",
    "mexico": "mexico",
    "peru": "peru",
}


def resolve_parameters(sql: str, country: str) -> str:
    """Resolve {parameter} placeholders in SQL."""
    # Default parameters per country
    default_params = {
        "ecuador": {"start_period": "202201"},
        "peru": {"start_period": "202201"},
    }

    params = default_params.get(country, {})

    for param, value in params.items():
        sql = sql.replace(f"{{{param}}}", str(value))

    return sql


def remove_aliases_and_lowercase(sql: str) -> str:
    """Remove AS aliases and convert to lowercase."""
    # Remove all AS aliases
    sql = re.sub(
        r'\b(\w+)\s+AS\s+\w+',
        r'\1',
        sql,
        flags=re.IGNORECASE
    )

    # Remove table aliases in FROM/JOIN clauses
    sql = re.sub(
        r'(FROM|JOIN)\s+(\w+\.\w+\.\w+|\w+\.\w+|\w+)\s+[A-Z]\b',
        r'\1 \2',
        sql,
        flags=re.IGNORECASE
    )

    # Convert to lowercase
    sql = sql.lower()

    return sql


def migrate_kwp_country_queries(
    source_base: Path,
    target_queries_dir: Path,
) -> None:
    """Migrate KWP per-country queries."""

    for source_country, target_country in COUNTRY_MAPPING.items():
        source_queries_dir = source_base / source_country / "queries"

        if not source_queries_dir.exists():
            continue

        sql_files = list(source_queries_dir.glob("*.sql"))
        if not sql_files:
            continue

        print(f"\n📍 Processing {source_country} ({target_country})...")

        # Create output directory
        output_dir = target_queries_dir / "countries" / target_country
        output_dir.mkdir(parents=True, exist_ok=True)

        for sql_file in sql_files:
            query_name = sql_file.stem
            query_content = sql_file.read_text(encoding="utf-8")

            # Resolve parameters first
            resolved_query = resolve_parameters(query_content, target_country)

            # Normalize: remove aliases and lowercase
            normalized_query = remove_aliases_and_lowercase(resolved_query)

            # Write to country-specific directory
            output_file = output_dir / f"{query_name}.sql"

            # If file exists, check if it's the same query
            if output_file.exists():
                existing = output_file.read_text(encoding="utf-8")
                if existing.strip() == normalized_query.strip():
                    print(f"  ⏭️  {query_name}.sql (unchanged)")
                    continue
                else:
                    print(f"  🔄 {query_name}.sql (merged/updated)")
            else:
                print(f"  ✅ {query_name}.sql (new)")

            output_file.write_text(normalized_query, encoding="utf-8")

        query_count = len(list(output_dir.glob("*.sql")))
        print(f"  🎉 Completed {target_country} ({query_count} total queries)")


def main():
    """Main migration entry point."""
    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    # KWP source (new per-country structure)
    kwp_base = Path("/home/jaalduna/Documents/projects/KWP-IntelligenceCalibration/bridge")

    # Target
    target_queries = project_root / "queries"

    print("🚀 Starting KWP per-country queries migration...")
    print(f"   Source: {kwp_base}")
    print(f"   Target: {target_queries}/countries/")

    # Run migration
    migrate_kwp_country_queries(
        source_base=kwp_base,
        target_queries_dir=target_queries,
    )

    print("\n✨ Migration complete!")


if __name__ == "__main__":
    main()
