# Missing Tables from data_shoveling - Added

## Summary

Added **104 new queries** with `SELECT *` from data_shoveling project.

These queries use `SELECT *` temporarily. On a computer with SQL Server access, run:
```bash
python scripts/get_table_schemas.py
```
This will query SQL Server and replace `SELECT *` with explicit column lists.

## Tables Added

### Product Hierarchy (7 countries: bolivia, brazil, chile, colombia, ecuador, mexico, peru)
- `a_fabricanteproduto` - Manufacturer dimension
- `a_marcaproduto` - Brand dimension  
- `a_produto` - Product master dimension
- `a_conteudoproduto` - Content/packaging dimension
- `a_subproduto` - Subproduct dimension
- `a_sector` - Sector dimension
- `a_tipocanal` - Channel type dimension
- `a_flgscanner` - Scanner flag dimension

### Reference Tables
- `pais` - Country dimension (all 9 countries)
- `paineis_individuos` - Panel individuals (7 Precios countries)
- `ps_latam` - PS LATAM reference (7 Precios countries)

### Data Mart Tables (7 Precios countries)
- `dbm_da2` - Data mart de análisis (analytical pre-aggregations)
- `dbm_da2_items` - Data mart item details
- `dbm_filtros` - Data mart filter configurations

### Country-Specific Tables
- `itf_bases` - Interface bases (Argentina only)
  - Used for: Product category to Base mapping for data quality filtering
  - Example: Drop specific Base/Category combinations like CHEE54SA
  
- `domicilio_animais` - Household pets (Brazil only)

- `rg_domicilios_pesos_bebes` - Baby weights (Mexico only)
- `rg_domicilios_pesos_beer` - Beer weights (Mexico only)

## Purpose of DBM and ITF Tables

### DBM Tables (Data Mart de Análisis)
**Purpose**: Pre-calculated analytical tables for performance optimization

- `dbm_da2` - Main analytical data mart with aggregated metrics
- `dbm_da2_items` - Item-level details for the data mart  
- `dbm_filtros` - Filter configurations for dashboards/reports

**Use case**: Fast analytical queries without joining raw tables every time.

### ITF Tables (Interface Tables)
**Purpose**: Argentina-specific data quality and business rules

- `itf_bases` - Maps product categories to "Base" classifications
- Used in Argentina's `drop_unwanted_bases()` function to filter out specific product/category combinations
- Example business rule: Exclude CHEE54SA base when category is in [1508, 5540, 5541, 5542, 5543]

## Query Distribution After Adding

| Country   | Queries | Change |
|-----------|---------|--------|
| argentina | 8       | +2 (pais, itf_bases) |
| bolivia   | 39      | +14 |
| brazil    | 36      | +15 (includes domicilio_animais) |
| cam       | 10      | +1 (pais) |
| chile     | 40      | +14 |
| colombia  | 40      | +14 |
| ecuador   | 36      | +14 |
| mexico    | 38      | +16 (includes bebes, beer weights) |
| peru      | 36      | +14 |
| **TOTAL** | **283** | **+104** |

## Next Steps

1. **Commit and push** these queries
2. **On computer with SQL Server access**, run:
   ```bash
   # Set environment variables
   export SQL_SERVER_HOST="your_server"
   export SQL_SERVER_DATABASE="your_database"
   export SQL_SERVER_USER="your_username"
   export SQL_SERVER_PASSWORD="your_password"
   
   # Run schema extraction
   python scripts/get_table_schemas.py
   ```
3. **Commit the updated queries** with explicit column lists
4. **Pull on this computer** to get the final versions

## Why Argentina/CAM Don't Have Precios Tables

Argentina and CAM only have **Elegibilidad** queries (6 basic tables):
- domicilios, j_atoscompra_new, paineis_domicilios, pre_mordom, rg_domicilios_pesos, rg_panelis

They **don't have Precios tables** like:
- DOLAR, NAC_ATO, A_Canal, LOC_psData_Compras, hato_cabecalho, etc.

**Reason**: Argentina and CAM likely don't participate in the Precios/KWP programs yet, or use different data infrastructure. They have country-specific processing logic (`Argentina.py`, `cam.py` in data_shoveling).
