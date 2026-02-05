#!/usr/bin/env python3
"""Add missing dimension tables from data_shoveling with SELECT * for now."""

from pathlib import Path


# Mapping: table_name -> (countries, is_fact_table)
TABLE_COUNTRIES = {
    # Product hierarchy (all Precios countries)
    "a_fabricanteproduto": (["bolivia", "brazil", "chile", "colombia", "ecuador", "mexico", "peru"], False),
    "a_marcaproduto": (["bolivia", "brazil", "chile", "colombia", "ecuador", "mexico", "peru"], False),
    "a_produto": (["bolivia", "brazil", "chile", "colombia", "ecuador", "mexico", "peru"], False),
    "a_conteudoproduto": (["bolivia", "brazil", "chile", "colombia", "ecuador", "mexico", "peru"], False),
    "a_subproduto": (["bolivia", "brazil", "chile", "colombia", "ecuador", "mexico", "peru"], False),
    "a_sector": (["bolivia", "brazil", "chile", "colombia", "ecuador", "mexico", "peru"], False),
    "a_tipocanal": (["bolivia", "brazil", "chile", "colombia", "ecuador", "mexico", "peru"], False),
    "a_flgscanner": (["bolivia", "brazil", "chile", "colombia", "ecuador", "mexico", "peru"], False),

    # Country dimension (all countries)
    "pais": (["argentina", "bolivia", "brazil", "cam", "chile", "colombia", "ecuador", "mexico", "peru"], False),

    # Panel individuals (countries with panel data)
    "paineis_individuos": (["bolivia", "brazil", "chile", "colombia", "ecuador", "mexico", "peru"], False),

    # Special pesos tables
    "rg_domicilios_pesos_bebes": (["mexico"], True),  # Mexico baby weights
    "rg_domicilios_pesos_beer": (["mexico"], True),   # Mexico beer weights
    "domicilio_animais": (["brazil"], False),          # Brazil pets

    # Argentina-specific
    "itf_bases": (["argentina"], False),

    # Data mart tables (analytical, likely all Precios countries)
    "dbm_da2": (["bolivia", "brazil", "chile", "colombia", "ecuador", "mexico", "peru"], False),
    "dbm_da2_items": (["bolivia", "brazil", "chile", "colombia", "ecuador", "mexico", "peru"], False),
    "dbm_filtros": (["bolivia", "brazil", "chile", "colombia", "ecuador", "mexico", "peru"], False),

    # PS_LATAM reference table
    "ps_latam": (["bolivia", "brazil", "chile", "colombia", "ecuador", "mexico", "peru"], False),
}


def create_select_star_query(table_name: str, country: str, is_fact: bool = False) -> str:
    """Create a SELECT * query with appropriate comment."""

    query_lines = [
        f"-- {country}: {table_name}",
        f"-- note: using select * - column list to be added after schema inspection",
        "",
        "select",
        "    *",
        f"from {table_name}"
    ]

    # Add time filter placeholder for fact tables
    if is_fact:
        query_lines.append("-- where <time_column> >= {start_param} and <time_column> <= {end_param}")

    query_lines.append("")

    return "\n".join(query_lines)


def main():
    """Main entry point."""
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    queries_base = project_root / "queries" / "countries"

    print("📝 Adding missing dimension tables from data_shoveling...\n")

    added_count = 0

    for table_name, (countries, is_fact) in sorted(TABLE_COUNTRIES.items()):
        print(f"\n📊 {table_name.upper()}")

        for country in countries:
            country_dir = queries_base / country
            output_file = country_dir / f"{table_name}.sql"

            if output_file.exists():
                print(f"  ⏭️  {country}: already exists")
                continue

            query_content = create_select_star_query(table_name, country, is_fact)
            output_file.write_text(query_content, encoding='utf-8')

            fact_marker = " [FACT]" if is_fact else ""
            print(f"  ✅ {country}: added{fact_marker}")
            added_count += 1

    print(f"\n✨ Added {added_count} queries across all countries")
    print("\n📌 Next steps:")
    print("   1. Commit and push these queries")
    print("   2. On computer with SQL Server access, run:")
    print("      python scripts/get_table_schemas.py")
    print("   3. This will query SQL Server and replace SELECT * with explicit columns")


if __name__ == "__main__":
    main()
