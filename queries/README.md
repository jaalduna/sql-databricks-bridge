# SQL Databricks Bridge - Query Organization

## Structure

Country-specific extraction queries organized by table name:

```
queries/
└── countries/
    ├── argentina/    (6 queries)
    ├── bolivia/      (25 queries)
    ├── brazil/       (21 queries)
    ├── cam/          (9 queries)
    ├── chile/        (26 queries)
    ├── colombia/     (26 queries)
    ├── ecuador/      (22 queries)
    ├── mexico/       (22 queries)
    └── peru/         (22 queries)
```

## Naming Convention

**Query filename = Table name**

- `j_atoscompra_new.sql` → queries `FROM j_atoscompra_new`
- `rg_panelis.sql` → queries `FROM rg_panelis`
- `domicilios.sql` → queries `FROM domicilios`

## Query Format

Simple extraction queries (ELT pattern):
- Explicit column lists (for data governance)
- No JOINs, no CASE statements, no calculations
- Parametrized time filters for fact tables

### Example - Fact Table

```sql
-- bolivia: j_atoscompra_new
-- columns: 27

select
    coef_01,
    data_compra,
    flg_scanner,
    idato,
    iddomicilio,
    periodo,
    ...
from j_atoscompra_new
where periodo >= {start_period} and periodo <= {end_period}
```

### Example - Dimension Table

```sql
-- bolivia: domicilios
-- columns: 3

select
    idcidade,
    iddomicilio,
    origem
from domicilios
```

## Country-Specific Schemas

Different countries have different column availability:

### j_atoscompra_new
- **Argentina**: 11 columns (basic Elegibilidad fields)
- **Other countries**: 27-28 columns (includes Precios fields: coef_01-03, preco, quantidade, etc.)

### rg_panelis  
- **Brazil**: 36 columns (includes bebe_mes01-12, br_* fields)
- **Other countries**: 12 columns (standard household panel fields)

### vw_artigoz
- **Bolivia/Brazil/CAM/Chile/Colombia**: 58 columns (includes mwp_* Master World Panel fields)
- **Ecuador/Mexico/Peru**: 38 columns (basic product master fields)

## Time Filters

Fact tables include parametrized time filters:

| Column | Filter | Tables |
|--------|--------|--------|
| `periodo` | `{start_period}` - `{end_period}` | j_atoscompra_new, dolar, rg_pets |
| `data_compra` | `{start_date}` - `{end_date}` | hato_cabecalho |
| `ano` | `{start_year}` - `{end_year}` | rg_panelis, rg_domicilios_pesos, pre_mordom |

## Usage

```python
from sql_databricks_bridge.core.country_query_loader import CountryAwareQueryLoader

loader = CountryAwareQueryLoader("queries")

# List available tables for a country
tables = loader.list_queries("bolivia")  # ['j_atoscompra_new', 'rg_panelis', ...]

# Get query for a table
sql = loader.get_query("j_atoscompra_new", "bolivia")
```

## Generation

Queries are auto-generated from existing queries using:

```bash
python scripts/generate_country_queries.py
```

This script:
1. Analyzes all existing queries per country
2. Extracts table names and column lists
3. Generates one query per table per country
4. Includes only columns available in that country's schema

Old queries are backed up in each country's `.backup/` directory.
