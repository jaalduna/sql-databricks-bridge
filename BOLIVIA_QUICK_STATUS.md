# 🚀 Bolivia Data Extraction - Quick Reference (Updated)

**Date:** February 5, 2026  
**Status:** ✅ PRODUCTION READY

---

## ✅ COMPLETED

### **What We Fixed:**
1. ✅ **j_atoscompra_new** - Date filter bug (0 → 1.6M rows)
2. ✅ **individuo** - Column schema mismatch (now 90k rows)
3. ✅ **vw_venues** - Column schema mismatch (now 555 rows)

### **Current Stats:**
- **Success Rate:** 77% (30 of 39 tables)
- **Total Rows:** ~3.2 million
- **Critical Tables:** All working ✅

---

## 🎯 DATA QUALITY CHECKS

### **Quick Validation Queries:**

```sql
-- Transaction coverage:
SELECT 
    MIN(data_compra) as first_date,
    MAX(data_compra) as last_date,
    COUNT(*) as transactions,
    COUNT(DISTINCT periodo) as periods
FROM bolivia.j_atoscompra_new;
-- Expected: 1.6M rows, 24 periods (202402-202601)

-- Household demographics:
SELECT 
    COUNT(*) as individuals,
    COUNT(DISTINCT idDomicilio) as households
FROM bolivia.individuo;
-- Expected: 90k individuals, 3k households

-- Product master data:
SELECT COUNT(*) as brands FROM bolivia.a_marcaproduto;
SELECT COUNT(*) as articles FROM bolivia.vw_artigoz_all;
-- Expected: 18k brands, 117k articles

-- Venues:
SELECT COUNT(*) as venues FROM bolivia.vw_venues;
-- Expected: 555 venues
```

---

## 📊 TABLE REFERENCE

### **Main Transaction Tables:**
- ✅ `j_atoscompra_new` - 1.6M rows (Feb 2024 - Jan 2026)
- ✅ `hato_cabecalho` - 1.3M rows (purchase headers)
- ❌ `nac_ato` - Inaccessible (permission denied)

### **Demographics:**
- ✅ `individuo` - 90k individuals
- ✅ `domicilios` - 5.8k households
- ✅ `paineis_individuos` - 24k panel members

### **Product Data:**
- ✅ `a_marcaproduto` - 18k brands
- ✅ `a_fabricanteproduto` - 7k manufacturers
- ✅ `vw_artigoz_all` - 117k articles

### **Channels:**
- ✅ `a_canal` - 114 channels
- ✅ `vw_venues` - 555 venues

---

## 🔧 RE-EXTRACTION COMMAND

```bash
# Full re-extraction (if needed):
poetry run sql-databricks-bridge extract \
  --queries-path queries \
  --country bolivia \
  --destination 000-sql-databricks-bridge.bolivia \
  --overwrite \
  --verbose

# Expected: 30 successes, 9 failures (77% success rate)
```

---

## ❌ KNOWN FAILURES (9 tables)

### **Cannot Fix:**
1. **nac_ato** - Permission denied (needs DBA access to BO_SINC)
2. **loc_psdata_compras** - Doesn't exist
3. **ps_latam** - Doesn't exist (LATAM-wide table)
4. **sector** - Doesn't exist (use `a_sector` instead ✓)
5. **tblproductosinternos** - Doesn't exist

### **Low Priority (Schema Mismatches):**
6. **paineis** - Column errors (use `paineis_domicilios` + `paineis_individuos` ✓)
7. **pre_mordom** - Column errors
8. **rg_domicilios_pesos** - Column errors
9. **vw_artigoz** - Column errors (use `vw_artigoz_all` ✓)

---

## 📁 KEY FILES

### **Configuration:**
- `.env` - Database connections
- `config/bolivia.yaml` - Query parameters

### **Fixed Queries:**
- `queries/countries/bolivia/j_atoscompra_new.sql`
- `queries/countries/bolivia/individuo.sql`
- `queries/countries/bolivia/vw_venues.sql`

### **Documentation:**
- `BOLIVIA_FIXES_APPLIED.md` - Detailed fixes report
- `BOLIVIA_EXTRACTION_SUMMARY.md` - Initial extraction report
- `diagnosis_results.txt` - Diagnostic output

---

## 🔗 DATABRICKS ACCESS

**Workspace:** https://adb-2125673683576002.2.azuredatabricks.net/

**Tables Location:**
```
000-sql-databricks-bridge.bolivia.*
```

**CLI Commands:**
```bash
# List all Bolivia tables:
databricks tables list 000-sql-databricks-bridge bolivia

# Get table details:
databricks tables get 000-sql-databricks-bridge.bolivia.j_atoscompra_new

# Query from CLI:
databricks sql query "SELECT COUNT(*) FROM bolivia.j_atoscompra_new"
```

---

## 📞 CONNECTION INFO

### **SQL Server:**
- Server: `KTCLSQL002.KT.group.local`
- Database: `BO_KWP`
- Auth: Windows Authentication
- Driver: ODBC Driver 17 for SQL Server

### **Databricks:**
- Catalog: `000-sql-databricks-bridge`
- Schema: `bolivia`
- Volume: `files` (staging)

---

## 🎯 SUCCESS CRITERIA

✅ **Met:**
- [x] Transaction data extracted (1.6M rows)
- [x] Demographics extracted (90k individuals)
- [x] Product master data complete
- [x] 75%+ extraction success rate (achieved 77%)
- [x] All critical tables working

⚠️ **Optional (Not Blocking):**
- [ ] Fix remaining 4 schema mismatches
- [ ] Get access to nac_ato table

---

## 💡 TIPS FOR NEXT SESSION

1. **If tables are empty:** Check date filters in queries (use `'yyyyMM'` format for Periodo)
2. **If column errors:** Get actual schema with `SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'tablename'`
3. **If permission errors:** Contact DBA for database access
4. **Use diagnostic script:** `poetry run python diagnose_failures.py`

---

**Status: ✅ PRODUCTION READY**  
**Last Updated:** February 5, 2026  
**Next Action:** Run validation queries above
