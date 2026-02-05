#!/usr/bin/env python3
"""Merge Precios queries into sql-databricks-bridge.

Resolves country parameters, normalizes to lowercase, removes aliases.
"""

import re
from pathlib import Path
from typing import Any

import yaml


# Country code to full name mapping
COUNTRY_CODE_TO_NAME = {
    "BO": "bolivia",
    "BR": "brazil",
    "CL": "chile",
    "CO": "colombia",
    "EC": "ecuador",
    "MX": "mexico",
    "PE": "peru",
}


def merge_dicts(base: dict, override: dict) -> dict:
    """Deep merge two dictionaries."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def flatten_dict(d: dict, parent_key: str = "", sep: str = ".") -> dict[str, Any]:
    """Flatten nested dictionary for parameter resolution."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def resolve_query(query_template: str, params: dict) -> str:
    """Resolve {param} placeholders in SQL query."""
    resolved = query_template

    # Find all {param} placeholders
    pattern = r"\{([^}]+)\}"
    matches = re.findall(pattern, query_template)

    for param_path in matches:
        if param_path in params:
            value = params[param_path]
            # Convert value to string
            if isinstance(value, (list, dict)):
                continue
            resolved = resolved.replace(f"{{{param_path}}}", str(value))

    return resolved


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
    # Pattern: FROM table_name alias_letter
    sql = re.sub(
        r'(FROM|JOIN)\s+(\w+\.\w+\.\w+|\w+\.\w+|\w+)\s+[A-Z]\b',
        r'\1 \2',
        sql,
        flags=re.IGNORECASE
    )

    # Convert to lowercase
    sql = sql.lower()

    return sql


def load_country_config(
    common_path: Path,
    country_path: Path,
) -> dict:
    """Load and merge common + country-specific configuration."""
    with open(common_path, "r", encoding="utf-8") as f:
        common_params = yaml.safe_load(f) or {}

    with open(country_path, "r", encoding="utf-8") as f:
        country_params = yaml.safe_load(f) or {}

    # Merge (country overrides common)
    merged = merge_dicts(common_params, country_params)

    return merged


def migrate_precios_queries(
    source_queries_dir: Path,
    source_config_dir: Path,
    target_queries_dir: Path,
    countries: list[str],
) -> None:
    """Migrate Precios queries to country-specific resolved queries."""
    common_config_path = source_config_dir / "common_params.yaml"

    for country_code in countries:
        # Map country code to full name
        country_name = COUNTRY_CODE_TO_NAME.get(country_code, country_code.lower())

        print(f"\n📍 Processing {country_code} ({country_name})...")

        # Load merged config
        country_config_path = source_config_dir / f"{country_code}.yaml"
        if not country_config_path.exists():
            print(f"  ⚠️  Config not found: {country_config_path}, skipping")
            continue

        merged_config = load_country_config(common_config_path, country_config_path)
        flat_params = flatten_dict(merged_config)

        # Create output directory (use full country name)
        output_dir = target_queries_dir / "countries" / country_name
        output_dir.mkdir(parents=True, exist_ok=True)

        # Process all .sql files
        sql_files = list(source_queries_dir.glob("*.sql"))

        for sql_file in sql_files:
            # Extract query name (remove extract_ prefix if exists)
            query_name = sql_file.stem
            if query_name.startswith("extract_"):
                query_name = query_name[8:]  # Remove "extract_" prefix

            query_template = sql_file.read_text(encoding="utf-8")

            # Resolve parameters
            resolved_query = resolve_query(query_template, flat_params)

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
        print(f"  🎉 Completed {country_name} ({query_count} total queries)")


def main():
    """Main migration entry point."""
    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    # Precios source
    precios_base = Path("/home/jaalduna/Documents/projects/Precios/bridge")
    precios_queries = precios_base / "queries"
    precios_config = precios_base / "configs"

    # Target
    target_queries = project_root / "queries"

    # Countries in Precios (check which config files exist)
    available_countries = []
    for config_file in precios_config.glob("*.yaml"):
        if config_file.stem != "common_params":
            available_countries.append(config_file.stem)

    print("🚀 Starting Precios queries migration...")
    print(f"   Source: {precios_queries}")
    print(f"   Target: {target_queries}/countries/")
    print(f"   Countries: {', '.join(available_countries)}")

    # Run migration
    migrate_precios_queries(
        source_queries_dir=precios_queries,
        source_config_dir=precios_config,
        target_queries_dir=target_queries,
        countries=available_countries,
    )

    print("\n✨ Migration complete!")


if __name__ == "__main__":
    main()
