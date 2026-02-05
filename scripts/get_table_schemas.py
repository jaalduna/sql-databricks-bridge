#!/usr/bin/env python3
"""Query SQL Server to get table schemas and replace SELECT * with explicit columns.

Run this script on a computer with SQL Server access to get actual column names.
"""

import pyodbc
from pathlib import Path
from typing import List
import os


def get_table_columns(cursor, table_name: str, schema: str = "dbo") -> List[str]:
    """Query SQL Server INFORMATION_SCHEMA to get column names."""
    query = """
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ?
      AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
    """

    try:
        cursor.execute(query, (schema, table_name))
        columns = [row.COLUMN_NAME.lower() for row in cursor.fetchall()]
        return columns
    except Exception as e:
        print(f"  ⚠️  Error getting columns for {schema}.{table_name}: {e}")
        return []


def replace_select_star(query_file: Path, columns: List[str], table_name: str, country: str):
    """Replace SELECT * with explicit column list."""

    if not columns:
        print(f"  ⚠️  No columns found, keeping SELECT *")
        return

    # Build new query
    query_lines = [
        f"-- {country}: {table_name}",
        f"-- columns: {len(columns)}",
        "",
        "select"
    ]

    # Add columns
    for i, col in enumerate(columns):
        suffix = "," if i < len(columns) - 1 else ""
        query_lines.append(f"    {col}{suffix}")

    query_lines.append(f"from {table_name}")
    query_lines.append("")

    # Write back
    query_file.write_text("\n".join(query_lines), encoding='utf-8')
    print(f"  ✅ Replaced SELECT * with {len(columns)} columns")


def main():
    """Main entry point."""
    # Get SQL Server connection from environment
    server = os.getenv("SQL_SERVER_HOST")
    database = os.getenv("SQL_SERVER_DATABASE")
    username = os.getenv("SQL_SERVER_USER")
    password = os.getenv("SQL_SERVER_PASSWORD")

    if not all([server, database, username, password]):
        print("❌ Missing SQL Server credentials. Please set environment variables:")
        print("   SQL_SERVER_HOST")
        print("   SQL_SERVER_DATABASE")
        print("   SQL_SERVER_USER")
        print("   SQL_SERVER_PASSWORD")
        return

    # Connect to SQL Server
    print(f"🔌 Connecting to SQL Server: {server}/{database}")
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("✅ Connected successfully\n")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return

    # Find all queries with SELECT *
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    queries_base = project_root / "queries" / "countries"

    tables_processed = {}

    for country_dir in sorted(queries_base.iterdir()):
        if not country_dir.is_dir() or country_dir.name.startswith('.'):
            continue

        country = country_dir.name

        for query_file in sorted(country_dir.glob("*.sql")):
            content = query_file.read_text(encoding='utf-8')

            # Check if it's a SELECT * query
            if "select\n    *\n" not in content.lower():
                continue

            table_name = query_file.stem

            # Get columns (cache per table)
            if table_name not in tables_processed:
                print(f"📊 {table_name.upper()}")
                columns = get_table_columns(cursor, table_name)
                tables_processed[table_name] = columns
            else:
                columns = tables_processed[table_name]

            if columns:
                replace_select_star(query_file, columns, table_name, country)

    cursor.close()
    conn.close()

    print(f"\n✨ Processed {len(tables_processed)} tables")
    print("\n📌 Next steps:")
    print("   1. Review the updated queries")
    print("   2. Commit and push the changes")
    print("   3. Pull on the other computer")


if __name__ == "__main__":
    main()
