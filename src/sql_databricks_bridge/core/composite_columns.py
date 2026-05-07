"""Utilities for composite level1 column handling in diff-sync.

When ``level1_column`` contains ``+`` (e.g. ``ano+mes+idproduto``), the
fingerprint GROUP BY uses multiple columns and values are stored as
pipe-separated strings (``2025|4|12345``).
"""

from __future__ import annotations


def is_composite(level1_column: str) -> bool:
    """Return True if level1_column uses composite syntax (contains '+')."""
    return "+" in level1_column


def parse_columns(level1_column: str) -> list[str]:
    """Split composite column spec into individual column names.

    ``'ano+mes+idproduto'`` -> ``['ano', 'mes', 'idproduto']``
    ``'periodo'`` -> ``['periodo']``
    """
    return [c.strip() for c in level1_column.split("+") if c.strip()]


def sql_server_value_expr(columns: list[str]) -> str:
    """Build the SELECT expression that produces the fingerprint value string.

    Single: ``CAST(periodo AS VARCHAR(100))``
    Composite: ``CONCAT_WS('|', CAST(ano AS VARCHAR(100)), ...)``
    """
    if len(columns) == 1:
        return f"CAST({columns[0]} AS VARCHAR(100))"
    casts = ", ".join(f"CAST({c} AS VARCHAR(100))" for c in columns)
    return f"CONCAT_WS('|', {casts})"


def sql_server_group_expr(columns: list[str]) -> str:
    """Build the GROUP BY / ORDER BY expression for SQL Server."""
    return ", ".join(columns)


def sql_server_where_in(columns: list[str], values: list[str]) -> str:
    """Build WHERE clause for SQL Server extraction.

    Single: ``CAST(periodo AS VARCHAR(100)) IN ('202501', '202502')``
    Composite: ``CONCAT_WS('|', CAST(ano ...), ...) IN ('2025|4|12345', ...)``
    """
    values_csv = ", ".join(f"'{v}'" for v in values)
    expr = sql_server_value_expr(columns)
    return f"{expr} IN ({values_csv})"


def databricks_where_in(columns: list[str], values: list[str]) -> str:
    """Build WHERE clause for Databricks DELETE.

    Single: ``CAST(periodo AS STRING) IN ('202501', '202502')``
    Composite: ``CONCAT_WS('|', CAST(ano AS STRING), ...) IN ('2025|4|...', ...)``
    """
    values_csv = ", ".join(f"'{v}'" for v in values)
    if len(columns) == 1:
        return f"CAST({columns[0]} AS STRING) IN ({values_csv})"
    casts = ", ".join(f"CAST(`{c}` AS STRING)" for c in columns)
    return f"CONCAT_WS('|', {casts}) IN ({values_csv})"
