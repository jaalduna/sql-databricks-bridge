# Delta Tables - Extraction to Unity Catalog

## Overview

The extraction flow writes SQL Server query results directly as **Delta tables** in Databricks Unity Catalog, using a **stage-then-CTAS** pattern. This replaces the previous approach of uploading parquet files to Databricks Volumes.

**Before:** `SQL Server → chunks → /Volumes/.../br/customers/part_00000.parquet`
**After:** `SQL Server → staging parquet → CREATE OR REPLACE TABLE kpi_prd_01.bronze.br_customers`

### Benefits

- Tables are immediately queryable via SQL (no `read_files()` needed downstream)
- Atomic overwrites via `CREATE OR REPLACE TABLE`
- Tables participate in Unity Catalog governance (permissions, lineage, discovery)
- Schema is inferred automatically from the parquet staging file

---

## Table Naming Convention

Tables follow the pattern:

```
{catalog}.{schema}.{country}_{query_name}
```

| Component | Source | Example |
|-----------|--------|---------|
| `catalog` | `DATABRICKS_CATALOG` env var or `--destination` | `kpi_prd_01` |
| `schema` | `DATABRICKS_SCHEMA` env var or `--destination` | `bronze` |
| `country` | `--country` CLI arg | `cl` |
| `query_name` | SQL filename (without `.sql`) | `customers` |

**Examples:**

| Country | Query file | Table |
|---------|-----------|-------|
| CL | `customers.sql` | `kpi_prd_01.bronze.cl_customers` |
| BR | `sales.sql` | `kpi_prd_01.bronze.br_sales` |
| MX | `products.sql` | `kpi_prd_01.bronze.mx_products` |

---

## How It Works (Stage-then-CTAS)

```
1. Extract data from SQL Server
         │
         ▼
2. Combine chunks into single Polars DataFrame
         │
         ▼
3. Upload as temporary parquet to Volume staging path
   /Volumes/{catalog}/{schema}/{volume}/_staging/{country}_{query}.parquet
         │
         ▼
4. Execute CTAS:
   CREATE OR REPLACE TABLE {catalog}.{schema}.{country}_{query}
   AS SELECT * FROM read_files('{staging_path}', format => 'parquet')
         │
         ▼
5. Delete staging file (best-effort cleanup)
         │
         ▼
6. Table is ready to query
```

The staging parquet is uploaded to the Volume path configured via `DATABRICKS_VOLUME`. The final Delta table lives in Unity Catalog at `{catalog}.{schema}.{country}_{query}`.

---

## CLI Usage

### Basic Extraction

Uses default catalog/schema from environment variables (`DATABRICKS_CATALOG`, `DATABRICKS_SCHEMA`):

```bash
sql-databricks-bridge extract \
  --queries-path ./queries \
  --config-path ./config \
  --country CL
```

This writes tables like `kpi_prd_01.bronze.cl_{query_name}` (assuming `DATABRICKS_CATALOG=kpi_prd_01` and `DATABRICKS_SCHEMA=bronze`).

### Override Catalog and Schema

Use `--destination catalog.schema` to override the defaults:

```bash
sql-databricks-bridge extract \
  --queries-path ./queries \
  --config-path ./config \
  --country CL \
  --destination kpi_dev_01.bronze
```

This writes to `kpi_dev_01.bronze.cl_{query_name}` regardless of env var settings.

### Run Specific Queries

```bash
sql-databricks-bridge extract \
  --queries-path ./queries \
  --config-path ./config \
  --country BR \
  --query customers \
  --query sales
```

### Overwrite Existing Tables

By default, existing tables are skipped. Use `--overwrite` to replace them:

```bash
sql-databricks-bridge extract \
  --queries-path ./queries \
  --config-path ./config \
  --country CL \
  --overwrite
```

### Limit Rows (Testing)

Use `--limit` to extract only N rows per query (wraps with `SELECT TOP N`):

```bash
sql-databricks-bridge extract \
  --queries-path ./queries \
  --config-path ./config \
  --country CL \
  --limit 100
```

### Override Parameters

Pass extra parameters that override YAML config values:

```bash
sql-databricks-bridge extract \
  --queries-path ./queries \
  --config-path ./config \
  --country CL \
  --param precios.mes_ini=20250101 \
  --param precios.mes_fin=20250131
```

### Full Example

```bash
sql-databricks-bridge extract \
  --queries-path ./queries \
  --config-path ./config \
  --country CL \
  --destination kpi_dev_01.bronze \
  --query customers \
  --query sales \
  --limit 1000 \
  --overwrite \
  --verbose
```

Output:

```
Starting extraction job: abc123-def456-...
  Country: CL
  Queries: 2
  Target: Delta tables (kpi_dev_01.bronze)
  Row limit: 1,000 rows per query (testing mode)

 ✓ customers: 1,000 rows → kpi_dev_01.bronze.cl_customers
 ✓ sales: 1,000 rows → kpi_dev_01.bronze.cl_sales

Extraction complete!
```

---

## API Usage

### Start Extraction

```bash
curl -X POST http://localhost:8000/extract \
  -H "Authorization: Bearer your-api-token" \
  -H "Content-Type: application/json" \
  -d '{
    "queries_path": "/path/to/queries",
    "config_path": "/path/to/config",
    "country": "CL",
    "target_catalog": "kpi_dev_01",
    "target_schema": "bronze",
    "queries": ["customers", "sales"],
    "overwrite": true,
    "chunk_size": 100000
  }'
```

**Response:**

```json
{
  "job_id": "abc123-def456-...",
  "status": "pending",
  "message": "Extraction job started",
  "queries_count": 2
}
```

### Request Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `queries_path` | Yes | - | Path to directory with `.sql` files |
| `config_path` | Yes | - | Path to directory with YAML configs |
| `country` | Yes | - | Country code for parameter resolution |
| `destination` | No | `null` | Legacy field (not needed for Delta tables) |
| `target_catalog` | No | `null` | Override `DATABRICKS_CATALOG` |
| `target_schema` | No | `null` | Override `DATABRICKS_SCHEMA` |
| `queries` | No | all | List of specific queries to run |
| `overwrite` | No | `false` | Replace existing tables |
| `chunk_size` | No | `100000` | Rows per SQL Server extraction chunk |

---

## Environment Variables

These variables control where Delta tables are written:

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABRICKS_HOST` | Workspace URL | `https://adb-123.2.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | PAT or leave empty for SP auth | `dapi...` |
| `DATABRICKS_WAREHOUSE_ID` | SQL Warehouse for CTAS execution | `08fa9256f365f47a` |
| `DATABRICKS_CATALOG` | Default catalog for output tables | `kpi_prd_01` |
| `DATABRICKS_SCHEMA` | Default schema for output tables | `bronze` |
| `DATABRICKS_VOLUME` | Volume name for staging parquet files | `raw_data` |

The Volume (`DATABRICKS_VOLUME`) is only used for temporary staging. The final output is always a Delta table in `{catalog}.{schema}`.

---

## Querying Output Tables

After extraction, tables are standard Delta tables in Unity Catalog:

```sql
-- Direct query
SELECT * FROM kpi_prd_01.bronze.cl_customers LIMIT 10;

-- Cross-country join
SELECT cl.*, br.*
FROM kpi_prd_01.bronze.cl_sales cl
JOIN kpi_prd_01.bronze.br_sales br
  ON cl.product_id = br.product_id;

-- Check table metadata
DESCRIBE TABLE EXTENDED kpi_prd_01.bronze.cl_customers;

-- Show all country tables
SHOW TABLES IN kpi_prd_01.bronze LIKE '*_customers';
```

---

## DeltaTableWriter (Internal Architecture)

The `DeltaTableWriter` class (`src/sql_databricks_bridge/core/delta_writer.py`) handles the write logic:

```
DeltaTableWriter
├── resolve_table_name(query, country, catalog?, schema?) → str
│   Returns: "{catalog}.{schema}.{country}_{query}"
│
├── write_dataframe(df, query, country, catalog?, schema?) → WriteResult
│   1. Stages parquet to Volume/_staging/
│   2. Executes CREATE OR REPLACE TABLE ... AS SELECT * FROM read_files()
│   3. Cleans up staging file
│
└── table_exists(table_name) → bool
    Runs DESCRIBE TABLE to check existence
```

The previous `Uploader` class (`core/uploader.py`) remains in the codebase but is no longer used by the extraction flow.
