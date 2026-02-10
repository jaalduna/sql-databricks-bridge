# 🇧🇷 Brazil Data Extraction - Quick Start

**Status:** ✅ Ready to extract (Permissions granted)  
**Server:** KTCLSQL003.KT.group.local  
**Database:** BR_KWP  
**Databricks Target:** 000-sql-databricks-bridge.brazil

---

## 🚀 Extraction Commands

### Extract All Brazil Tables (35 tables)

```bash
poetry run sql-databricks-bridge extract \
  --queries-path queries \
  --country brazil \
  --destination 000-sql-databricks-bridge.brazil \
  --verbose
```

### Extract with Row Limit (Testing)

```bash
# Test extraction with first 1000 rows only
poetry run sql-databricks-bridge extract \
  --queries-path queries \
  --country brazil \
  --limit 1000 \
  --destination 000-sql-databricks-bridge.brazil \
  --verbose
```

### Extract Specific Tables Only

```bash
# Extract just critical tables
poetry run sql-databricks-bridge extract \
  --queries-path queries \
  --country brazil \
  --query hato_cabecalho \
  --query nac_ato \
  --query domicilios \
  --query paineis_domicilios \
  --destination 000-sql-databricks-bridge.brazil \
  --verbose
```

---

## 📊 Brazil Tables (35 total)

**Dimension Tables (12):**
- a_accesocanal
- a_canal
- a_conteudoproduto
- a_fabricanteproduto
- a_flgscanner
- a_formapagto
- a_marcaproduto
- a_produto
- a_produtocoeficienteprincipal
- a_sector
- a_subproduto
- a_tipocanal

**Fact/Transaction Tables:**
- hato_cabecalho (Purchase headers)
- nac_ato (Transaction details) - ⚠️ Uses BR_NAC.dbo.nac_ato
- j_atoscompra_new (Purchase acts)

**Panel/Household Tables:**
- domicilios
- domicilio_animais
- paineis_domicilios
- paineis_individuos
- pre_mordom
- rg_domicilios_pesos
- rg_panelis
- rg_pets

**Reference/Lookup Tables:**
- dbm_da2
- dbm_da2_items
- dbm_filtros
- dolar
- dt_mes
- grupo
- htipo_ato
- individuo
- pais
- sector
- tblproductosinternos
- ventasuelta
- vw_artigoz
- vw_artigoz_all
- vw_venues

---

## 🔐 Permissions Status

✅ **BR_KWP** - Main database access (granted)  
✅ **BR_NAC** - `db_datareader` role (granted)  
✅ **PS_LATAM** - SELECT on loc_psdata_* tables (granted)

---

## 📝 Important Notes

### Cross-Database Queries

Some queries reference other databases:

1. **nac_ato** - Located in `BR_NAC.dbo.nac_ato` (separate database)
2. **loc_psdata_compras** - Located in `PS_LATAM.dbo.loc_psdata_compras` (server-wide)
3. **loc_psdata_procesado** - Located in `PS_LATAM.dbo.loc_psdata_procesado` (server-wide)

**Note:** PS_LATAM tables should be extracted using server-level queries:
```bash
poetry run python extract_ps_latam_servers.py --server 3
```

This extracts to: `000-sql-databricks-bridge.KTCLSQL003` (not `.brazil`)

---

## ⏱️ Estimated Extraction Time

Based on Bolivia extraction performance:
- **Full extraction:** 30-45 minutes
- **With limit (1000 rows):** 2-5 minutes
- **Large tables (nac_ato):** May take 15-20 minutes alone

---

## ✅ Verification After Extraction

```sql
-- Check if schema was created
SHOW SCHEMAS IN `000-sql-databricks-bridge`;

-- List all extracted tables
SHOW TABLES IN `000-sql-databricks-bridge`.brazil;

-- Verify row counts
SELECT 'hato_cabecalho' as table_name, COUNT(*) as row_count 
FROM `000-sql-databricks-bridge`.brazil.hato_cabecalho
UNION ALL
SELECT 'nac_ato', COUNT(*) 
FROM `000-sql-databricks-bridge`.brazil.nac_ato
UNION ALL
SELECT 'domicilios', COUNT(*) 
FROM `000-sql-databricks-bridge`.brazil.domicilios;
```

---

**Ready to extract!** Run the command above to start. 🚀
