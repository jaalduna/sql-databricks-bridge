# Brasil Missing Columns - FIXED ✅

**Date:** 2024-01-XX  
**Status:** All 5 queries updated with missing columns

## Summary of Changes

All missing columns have been verified to exist in SQL Server and added to the respective query files.

---

## 1. ✅ j_atoscompra_new.sql

**Previous:** 28 columns  
**Now:** 100 columns (ALL columns from SQL Server)

**Change:** Used `SELECT *` to capture all 100 columns instead of listing them individually.

**Key columns now included:**
- `factor_rw1` ✅
- `idapp` ✅
- `acceso_canal` ✅ (note: `acceso` not `acesso`)
- `data_compra` ✅
- Plus ~70 other columns

**File:** `queries/countries/brasil/j_atoscompra_new.sql`

---

## 2. ✅ hato_cabecalho.sql

**Previous:** 20 columns  
**Now:** 21 columns

**Changes:**
- ❌ **REMOVED** `data_compra` (per user request)
- ✅ **ADDED** `data_compra_utilizada` (replacement for data_compra)
- ✅ **ADDED** `idapp`

**WHERE clause updated:** Now uses `data_compra_utilizada` instead of `data_compra`

**File:** `queries/countries/brasil/hato_cabecalho.sql`

---

## 3. ✅ rg_domicilios_pesos.sql

**Previous:** 5 columns  
**Now:** 6 columns

**Changes:**
- ✅ **ADDED** `idpeso`

**File:** `queries/countries/brasil/rg_domicilios_pesos.sql`

---

## 4. ✅ rg_panelis.sql

**Previous:** 36 columns  
**Now:** 37 columns

**Changes:**
- ✅ **ADDED** `nsereg`

**File:** `queries/countries/brasil/rg_panelis.sql`

---

## 5. ✅ rg_pets.sql

**Previous:** 5 columns  
**Now:** 6 columns

**Changes:**
- ✅ **ADDED** `ano`

**File:** `queries/countries/brasil/rg_pets.sql`

---

## Verification Method

All columns were verified by:
1. Creating test tables in Databricks using `SELECT TOP 1 * FROM table`
2. Checking actual columns in test tables using `DESCRIBE TABLE`
3. Confirming each missing column exists in SQL Server

**Test tables created in Databricks:**
- `000-sql-databricks-bridge.brasil.test_j_atoscompra_all` (100 columns)
- `000-sql-databricks-bridge.brasil.test_hato_cabecalho_all` (56 columns)
- `000-sql-databricks-bridge.brasil.test_rg_panelis_all` (318 columns)
- `000-sql-databricks-bridge.brasil.test_rg_pets_all` (43 columns)
- `000-sql-databricks-bridge.brasil.test_rg_domicilios_pesos_all` (7 columns)

---

## Next Steps

### 1. Test Updated Queries (Limited Extraction)

Test each query with a small limit to verify:

```bash
# Test j_atoscompra_new (should now have 100 columns)
poetry run sql-databricks-bridge extract \
  --queries-path queries \
  --country brasil \
  --query j_atoscompra_new \
  --destination 000-sql-databricks-bridge.brasil \
  --limit 100 \
  --overwrite \
  --verbose

# Test hato_cabecalho (should have 21 columns, NO data_compra)
poetry run sql-databricks-bridge extract \
  --queries-path queries \
  --country brasil \
  --query hato_cabecalho \
  --destination 000-sql-databricks-bridge.brasil \
  --limit 100 \
  --overwrite \
  --verbose

# Test rg_domicilios_pesos (should have 6 columns including idpeso)
poetry run sql-databricks-bridge extract \
  --queries-path queries \
  --country brasil \
  --query rg_domicilios_pesos \
  --destination 000-sql-databricks-bridge.brasil \
  --limit 100 \
  --overwrite \
  --verbose

# Test rg_panelis (should have 37 columns including nsereg)
poetry run sql-databricks-bridge extract \
  --queries-path queries \
  --country brasil \
  --query rg_panelis \
  --destination 000-sql-databricks-bridge.brasil \
  --limit 100 \
  --overwrite \
  --verbose

# Test rg_pets (should have 6 columns including ano)
poetry run sql-databricks-bridge extract \
  --queries-path queries \
  --country brasil \
  --query rg_pets \
  --destination 000-sql-databricks-bridge.brasil \
  --limit 100 \
  --overwrite \
  --verbose
```

### 2. Verify Columns in Databricks

After extraction, verify column counts:

```python
from databricks.sdk import WorkspaceClient
client = WorkspaceClient()

tables = [
    'j_atoscompra_new',      # Expected: 100 columns
    'hato_cabecalho',        # Expected: 21 columns
    'rg_domicilios_pesos',   # Expected: 6 columns
    'rg_panelis',            # Expected: 37 columns
    'rg_pets'                # Expected: 6 columns
]

for table in tables:
    query = f"DESCRIBE TABLE `000-sql-databricks-bridge`.`brasil`.`{table}`"
    result = client.statement_execution.execute_statement(
        warehouse_id="08fa9256f365f47a",
        statement=query,
        wait_timeout="30s"
    )
    # Count and display columns
```

### 3. Full Production Extraction

Once verified, run full extraction for all Brasil tables:

```bash
poetry run sql-databricks-bridge extract \
  --queries-path queries \
  --country brasil \
  --destination 000-sql-databricks-bridge.brasil \
  --overwrite \
  --verbose
```

---

## Important Notes

1. **Lowercase Conversion:** All columns will be automatically converted to lowercase in Databricks by `delta_writer.py` (line 81)

2. **Column Name Casing:** 
   - SQL Server: `acceso_canal` (one 'c')
   - Databricks: `acceso_canal` (lowercase)

3. **hato_cabecalho Special Case:**
   - `data_compra` was REMOVED per user request
   - Use only `data_compra_utilizada` for date filtering

4. **Test Tables:** Can be deleted after verification - they served their purpose for column discovery

---

## Files Modified

1. `queries/countries/brasil/j_atoscompra_new.sql`
2. `queries/countries/brasil/hato_cabecalho.sql`
3. `queries/countries/brasil/rg_domicilios_pesos.sql`
4. `queries/countries/brasil/rg_panelis.sql`
5. `queries/countries/brasil/rg_pets.sql`

---

## Success Criteria ✅

- [x] All 5 query files updated
- [x] All missing columns verified to exist in SQL Server
- [x] `j_atoscompra_new` now uses SELECT * to get all 100 columns
- [x] `hato_cabecalho` has `data_compra_utilizada` and `idapp`, NO `data_compra`
- [x] `rg_domicilios_pesos` has `idpeso`
- [x] `rg_panelis` has `nsereg`
- [x] `rg_pets` has `ano`
- [ ] Test extraction successful (--limit 100) - PENDING
- [ ] Column counts verified in Databricks - PENDING
- [ ] Full extraction ready to run - PENDING
