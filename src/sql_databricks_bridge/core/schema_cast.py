"""Helpers for schema-aware INSERT OVERWRITE / INSERT INTO from parquet.

Used to build CAST expressions that handle two failure modes Databricks
hits when source parquet and target Delta schemas drift:

1. Column arity mismatch — source has fewer columns than target. Build
   the SELECT list from the target schema, emitting CAST(NULL AS T) for
   any column that does not exist in the source.

2. Cross-family type mismatch — e.g. source is TIMESTAMP_NTZ but target
   is INT (vestigial column with no real data). Databricks rejects even
   TRY_CAST at parse time for some cross-family pairs; emit
   CAST(NULL AS T) in those cases. Otherwise TRY_CAST gracefully nulls
   bad values at runtime.
"""
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sql_databricks_bridge.db.databricks import DatabricksClient

logger = logging.getLogger(__name__)


_NUMERIC_TYPES = {"tinyint", "smallint", "int", "integer", "bigint", "float", "double", "decimal"}
_TEMPORAL_TYPES = {"date", "timestamp", "timestamp_ntz"}
_BOOLEAN_TYPES = {"boolean"}


def type_family(t: str) -> str:
    t = t.lower().strip()
    if t.startswith("decimal"):
        return "decimal"
    if t in _NUMERIC_TYPES:
        return "numeric"
    if t in _TEMPORAL_TYPES:
        return "temporal"
    if t in _BOOLEAN_TYPES:
        return "boolean"
    return "other"


def cast_expr(col: str, source_type: str | None, target_type: str) -> str:
    """Build a CAST expression resilient to parse-time type mismatches.

    - If source is missing entirely (column not in source), emit
      ``CAST(NULL AS target) AS col``.
    - If source/target are cross-family incompatible (e.g. TIMESTAMP vs INT),
      Databricks rejects even TRY_CAST at parse time, so emit
      ``CAST(NULL AS target) AS col``.
    - Otherwise emit ``TRY_CAST(col AS target) AS col`` (NULL on failure).
    """
    if source_type is None:
        return f"CAST(NULL AS {target_type}) AS `{col}`"

    src_fam = type_family(source_type)
    tgt_fam = type_family(target_type)

    incompatible = {
        ("temporal", "numeric"),
        ("temporal", "decimal"),
        ("numeric", "temporal"),
        ("decimal", "temporal"),
        ("temporal", "boolean"),
        ("boolean", "temporal"),
    }
    if (src_fam, tgt_fam) in incompatible:
        return f"CAST(NULL AS {target_type}) AS `{col}`"

    return f"TRY_CAST(`{col}` AS {target_type}) AS `{col}`"


def get_parquet_schema(dbx_client: "DatabricksClient", staging_path: str) -> dict[str, str]:
    """Return column -> data_type map for a parquet staging path."""
    try:
        rows = dbx_client.execute_sql(
            f"SELECT * FROM read_files('{staging_path}', format => 'parquet') LIMIT 0"
        )
        if not rows:
            rows = dbx_client.execute_sql(
                f"DESCRIBE QUERY SELECT * FROM read_files('{staging_path}', format => 'parquet') LIMIT 0"
            )
        schema: dict[str, str] = {}
        for r in rows:
            name = r.get("col_name") or r.get("name")
            dtype = r.get("data_type") or r.get("dataType")
            if name and dtype:
                schema[str(name)] = str(dtype)
        return schema
    except Exception as e:
        logger.warning(f"Could not read parquet schema at {staging_path}: {e}")
        return {}
