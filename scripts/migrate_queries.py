#!/usr/bin/env python3
"""Migrate parameterized queries from KWP-IntelligenceCalibration to sql-databricks-bridge.

This script:
1. Reads common_params.yaml and country-specific YAML configs
2. Merges them (country-specific overrides common)
3. Resolves all {param} placeholders in SQL queries
4. Writes resolved queries to queries/countries/{country}/
"""

import re
from pathlib import Path
from typing import Any

import yaml


def merge_dicts(base: dict, override: dict) -> dict:
    """Deep merge two dictionaries (override takes precedence)."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def flatten_dict(d: dict, parent_key: str = "", sep: str = ".") -> dict[str, Any]:
    """Flatten nested dictionary for parameter resolution.

    Args:
        d: Dictionary to flatten.
        parent_key: Parent key prefix.
        sep: Separator between nested keys.

    Returns:
        Flattened dictionary where nested keys are joined with separator.

    Example:
        {'tables': {'hato_table': 'HAto_Cabecalho'}}
        becomes
        {'tables.hato_table': 'HAto_Cabecalho'}
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            # Flatten nested dicts recursively
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def resolve_query(query_template: str, params: dict) -> str:
    """Resolve {param} placeholders in SQL query.

    Args:
        query_template: SQL query with {param} placeholders.
        params: Flattened parameter dictionary.

    Returns:
        Resolved SQL query.
    """
    resolved = query_template

    # Find all {param} placeholders
    pattern = r"\{([^}]+)\}"
    matches = re.findall(pattern, query_template)

    for param_path in matches:
        if param_path in params:
            value = params[param_path]
            # Convert value to string
            if isinstance(value, (list, dict)):
                # Skip complex types (they're not meant for direct substitution)
                continue
            resolved = resolved.replace(f"{{{param_path}}}", str(value))

    return resolved


def load_country_config(
    common_path: Path,
    country_path: Path,
) -> dict:
    """Load and merge common + country-specific configuration.

    Args:
        common_path: Path to common_params.yaml.
        country_path: Path to country-specific YAML.

    Returns:
        Merged configuration dictionary.
    """
    with open(common_path, "r", encoding="utf-8") as f:
        common_params = yaml.safe_load(f) or {}

    with open(country_path, "r", encoding="utf-8") as f:
        country_params = yaml.safe_load(f) or {}

    # Merge (country overrides common)
    merged = merge_dicts(common_params, country_params)

    return merged


def migrate_queries(
    source_queries_dir: Path,
    source_config_dir: Path,
    target_queries_dir: Path,
    countries: list[str],
) -> None:
    """Migrate parameterized queries to country-specific resolved queries.

    Args:
        source_queries_dir: Directory containing parameterized .sql files.
        source_config_dir: Directory containing YAML configs.
        target_queries_dir: Root directory for output (will create countries/{country}/).
        countries: List of country names (e.g., ['Bolivia', 'Chile', 'Colombia']).
    """
    common_config_path = source_config_dir / "common_params.yaml"

    for country in countries:
        print(f"\n📍 Processing {country}...")

        # Load merged config
        country_config_path = source_config_dir / f"{country}.yaml"
        if not country_config_path.exists():
            print(f"  ⚠️  Config not found: {country_config_path}, skipping")
            continue

        merged_config = load_country_config(common_config_path, country_config_path)
        flat_params = flatten_dict(merged_config)

        # Create output directory
        output_dir = target_queries_dir / "countries" / country.lower()
        output_dir.mkdir(parents=True, exist_ok=True)

        # Process all .sql files
        sql_files = list(source_queries_dir.glob("*.sql"))
        print(f"  Found {len(sql_files)} SQL files")

        for sql_file in sql_files:
            query_name = sql_file.stem
            query_template = sql_file.read_text(encoding="utf-8")

            # Resolve parameters
            resolved_query = resolve_query(query_template, flat_params)

            # Write to country-specific directory
            output_file = output_dir / f"{query_name}.sql"
            output_file.write_text(resolved_query, encoding="utf-8")
            print(f"  ✅ {query_name}.sql")

        print(f"  🎉 Completed {country} ({len(sql_files)} queries)")


def main():
    """Main migration entry point."""
    # Paths
    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    source_base = Path("/home/jaalduna/Documents/projects/KWP-IntelligenceCalibration/bridge")
    source_queries = source_base / "queries"
    source_config = source_base / "config"

    target_queries = project_root / "queries"

    # Countries to migrate
    countries = ["Bolivia", "Chile", "Colombia"]

    print("🚀 Starting query migration...")
    print(f"   Source: {source_queries}")
    print(f"   Target: {target_queries}/countries/")

    # Run migration
    migrate_queries(
        source_queries_dir=source_queries,
        source_config_dir=source_config,
        target_queries_dir=target_queries,
        countries=countries,
    )

    print("\n✨ Migration complete!")
    print(f"\nNext steps:")
    print(f"1. Review generated queries in {target_queries}/countries/")
    print(f"2. Remove example queries from queries/common/ and queries/countries/")
    print(f"3. Run tests: PYTHONPATH=src pytest tests/unit/test_country_query_loader.py")


if __name__ == "__main__":
    main()
