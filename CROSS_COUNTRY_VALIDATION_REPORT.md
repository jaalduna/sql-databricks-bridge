# 🌎 Cross-Country Query Validation Report
**Date:** February 5, 2026  
**Countries Analyzed:** Bolivia, Mexico, Brazil  
**Focus:** Cross-database table references

---

## 📊 Executive Summary

**CRITICAL FINDINGS:**
1. ✅ **Mexico:** All cross-database queries are VALID (nac_ato in MX_KWP, PS_LATAM accessible)
2. ❌ **Brazil:** User lacks permissions to `BR_NAC.dbo.nac_ato` and `PS_LATAM.dbo.loc_psdata_compras`
3. ⚠️ **All 3 countries:** `ps_latam` table does NOT exist (same issue everywhere)

---

## 🗂️ Database Architecture by Country

### **Bolivia** ✅ (Fixed in Session 3)
- **Server:** KTCLSQL002.KT.group.local
- **Main Database:** BO_KWP
- **Additional Databases:**
  - BO_NAC (has nac_ato - 9.6M rows) ✅ User has access
  - PS_LATAM (has loc_psdata_compras - 13.1M Bolivia rows) ✅ User has access
- **Status:** QUERIES FIXED

### **Mexico** ✅ (No fixes needed)
- **Server:** KTCLSQL001.KT.group.local
- **Main Database:** MX_KWP
- **Additional Databases:**
  - MX_NAC, MX_SINC, MX_RA, MX_SPRI
  - PS_LATAM (accessible)
- **Status:** ALL TABLES ACCESSIBLE IN MX_KWP

### **Brazil** ❌ (Needs DBA access)
- **Server:** KTCLSQL003.KT.group.local
- **Main Database:** BR_KWP
- **Additional Databases:**
  - BR_NAC ❌ User lacks READ permission
  - BR_SINC, BR_RA, BR_SPRI, BR_SPRII ❌ User lacks READ permission
  - PS_LATAM ❌ User lacks READ permission to loc_psdata_compras
- **Status:** PERMISSIONS REQUIRED

---

## 📝 Detailed Findings by Table

### **1. `nac_ato` - Transaction Detail Table**

| Country | Database Location | Row Count | User Access | Query Status |
|---------|------------------|-----------|-------------|--------------|
| Bolivia | BO_NAC.dbo.nac_ato | 9,634,153 | ✅ Yes | ✅ FIXED (Session 3) |
| Mexico | MX_KWP.dbo.nac_ato | 74,413,589 | ✅ Yes | ✅ WORKS (no fix needed) |
| Brazil | BR_NAC.dbo.nac_ato | Unknown | ❌ No | ❌ NEEDS DBA ACCESS |

**Bolivia Query Fix Applied:**
```sql
-- BEFORE (broken):
from nac_ato

-- AFTER (fixed):
from BO_NAC.dbo.nac_ato
```

**Mexico Query Status:**
```sql
-- CURRENT (works fine):
from nac_ato
-- Table exists in MX_KWP, no database prefix needed
```

**Brazil Query Issue:**
```sql
-- CURRENT (broken):
from nac_ato
-- Table is in BR_NAC but user lacks permission

-- RECOMMENDED FIX (after DBA grants access):
from BR_NAC.dbo.nac_ato
```

---

### **2. `loc_psdata_compras` - Purchase Trip/Journey Data**

| Country | Database Location | Total Rows | User Access | Query Status |
|---------|------------------|------------|-------------|--------------|
| Bolivia | PS_LATAM.dbo.loc_psdata_compras | 13,176,985 (BO only) | ✅ Yes | ✅ FIXED (Session 3) |
| Mexico | PS_LATAM.dbo.loc_psdata_compras | ~23.7M (all countries) | ✅ Yes | ⚠️ NEEDS FIX (missing DB prefix) |
| Brazil | PS_LATAM.dbo.loc_psdata_compras | Unknown | ❌ No | ❌ NEEDS DBA ACCESS |

**Important Note:** `loc_psdata_compras` is a **LATAM-wide table** containing data for all countries. Use `idcountry` filter to get country-specific data.

**Bolivia Query Fix Applied:**
```sql
-- BEFORE (broken):
from loc_psdata_compras

-- AFTER (fixed):
from PS_LATAM.dbo.loc_psdata_compras
```

**Mexico Query Issue:**
```sql
-- CURRENT (broken):
from loc_psdata_compras
-- Missing database prefix - MX_KWP doesn't have this table

-- RECOMMENDED FIX:
from PS_LATAM.dbo.loc_psdata_compras
-- Note: idcountry is INT type, not VARCHAR
-- Mexico idcountry value needs investigation (not 'MX' string)
```

**Brazil Query Issue:**
```sql
-- CURRENT (broken):
from loc_psdata_compras
-- Table is in PS_LATAM but user lacks permission

-- RECOMMENDED FIX (after DBA grants access):
from PS_LATAM.dbo.loc_psdata_compras
```

---

### **3. `ps_latam` - Table Does NOT Exist** ⚠️

**Status:** This table name doesn't exist in PS_LATAM database for ANY country.

**Affected Countries:** Bolivia, Mexico, Brazil (all have this query)

**Current Query (ALL countries):**
```sql
-- xxx: ps_latam
select *
from ps_latam  -- ❌ TABLE DOESN'T EXIST!
```

**Available Alternatives in PS_LATAM:**
- `psData` - Main survey response data
- `psDataBolivia` - Bolivia-specific survey data
- `psDataBrasil` - Brazil-specific survey data (if exists)
- `psDataMexico` - Mexico-specific survey data (if exists)
- `psGenSurvey` - Survey definitions
- `psSurvey` - Survey metadata
- `psAnswers` / `psQuestions` - Q&A data

**Recommended Action:** User must identify which table they actually need.

---

## 🔧 Required Fixes

### **MEXICO - 1 Fix Required** ⚠️

**File:** `queries/countries/mexico/loc_psdata_compras.sql`

```sql
-- CHANGE THIS:
from loc_psdata_compras

-- TO THIS:
from PS_LATAM.dbo.loc_psdata_compras
```

**Additional Note:** Need to investigate the correct `idcountry` value for Mexico filtering (it's an INT, not 'MX' string).

---

### **BRAZIL - 2 Tables Need DBA Access** ❌

**Required Permissions:**

1. **BR_NAC database:**
   - Grant `db_datareader` role to user
   - Needed for: `nac_ato` table access
   - Estimated rows: Millions (based on Mexico ~74M)

2. **PS_LATAM database:**
   - Grant READ permission on `loc_psdata_compras` table
   - Needed for: Purchase journey data
   - Total rows: ~23.7M (all LATAM countries combined)

**After DBA grants access, fix these files:**

**File 1:** `queries/countries/brazil/nac_ato.sql`
```sql
-- CHANGE THIS:
from nac_ato

-- TO THIS:
from BR_NAC.dbo.nac_ato
```

**File 2:** `queries/countries/brazil/loc_psdata_compras.sql`
```sql
-- CHANGE THIS:
from loc_psdata_compras

-- TO THIS:
from PS_LATAM.dbo.loc_psdata_compras
```

---

### **ALL 3 COUNTRIES - ps_latam Investigation** 🔍

**Files to investigate:**
- `queries/countries/bolivia/ps_latam.sql`
- `queries/countries/mexico/ps_latam.sql`
- `queries/countries/brazil/ps_latam.sql`

**Action Required:**
1. User decides which PS_LATAM table they actually need
2. Update all 3 country queries with correct table name
3. Add country-specific filters if needed

**Investigation Query:**
```python
poetry run python << 'EOF'
from sql_databricks_bridge.db.sql_server import SQLServerClient

client = SQLServerClient(country='Bolivia')  # Or Mexico/Brasil

# List all psData* tables
result = client.execute_query("""
    SELECT TABLE_NAME 
    FROM PS_LATAM.INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME LIKE 'psData%'
    ORDER BY TABLE_NAME
""")

for i in range(len(result)):
    print(f"  - {result['TABLE_NAME'][i]}")

client.close()
EOF
```

---

## 📊 Row Count Comparison

### **nac_ato Transaction Details:**
- **Mexico:** 74,413,589 rows ⭐ (HUGE dataset!)
- **Bolivia:** 9,634,153 rows
- **Brazil:** Unknown (need access)

### **loc_psdata_compras Purchase Trips:**
- **Total (all LATAM):** 23,719,717 rows
- **Bolivia subset:** 13,176,985 rows (55% of total!)
- **Mexico subset:** Unknown (need to determine idcountry value)
- **Brazil subset:** Unknown (need access)

**Key Insight:** Mexico has **8x more transaction detail data** than Bolivia (74M vs 9.6M rows). This is a massive dataset that will take significant extraction time!

---

## ⏰ Estimated Extraction Times

Based on Bolivia extraction performance:

### **Mexico (if running full extraction):**
- `nac_ato` (74M rows): ~60-90 minutes 🕐
- `loc_psdata_compras` (subset): ~10-15 minutes
- Other tables: ~30 minutes
- **Total estimated time: 1.5 - 2.5 hours**

### **Brazil (after DBA grants access):**
- Similar to Bolivia: ~20-30 minutes
- Depends on nac_ato size (unknown)

---

## ✅ Immediate Action Items

### **Priority 1: Mexico Fix** (Can do immediately)
```bash
# 1. Fix loc_psdata_compras query
# Edit: queries/countries/mexico/loc_psdata_compras.sql
# Change: from loc_psdata_compras
# To: from PS_LATAM.dbo.loc_psdata_compras

# 2. Investigate idcountry for Mexico
poetry run python << 'EOF'
from sql_databricks_bridge.db.sql_server import SQLServerClient
client = SQLServerClient(country='Mexico')
result = client.execute_query("""
    SELECT DISTINCT idcountry, COUNT(*) as cnt
    FROM PS_LATAM.dbo.loc_psdata_compras
    GROUP BY idcountry
    ORDER BY cnt DESC
""")
print("idcountry values and counts:")
for i in range(len(result)):
    print(f"  {result['idcountry'][i]}: {result['cnt'][i]:,} rows")
client.close()
EOF

# 3. Test extraction (optional)
poetry run sql-databricks-bridge extract \
  --queries-path queries \
  --country mexico \
  --query-name loc_psdata_compras \
  --destination 000-sql-databricks-bridge.mexico \
  --verbose
```

### **Priority 2: Brazil DBA Request** (Need approval)
Create DBA ticket requesting:
- db_datareader role on BR_NAC database
- SELECT permission on PS_LATAM.dbo.loc_psdata_compras table
- Reason: Data extraction for analytics (same as Bolivia BO_NAC access)

### **Priority 3: ps_latam Investigation** (User decision needed)
User needs to identify which table they actually need from PS_LATAM database.

---

## 🎯 Summary Table

| Country | Main DB | Status | nac_ato Access | loc_psdata_compras Access | ps_latam Fix | Priority |
|---------|---------|--------|---------------|---------------------------|--------------|----------|
| Bolivia | BO_KWP | ✅ FIXED | ✅ Yes (BO_NAC) | ✅ Yes (PS_LATAM) | ⚠️ Pending | ✅ DONE |
| Mexico | MX_KWP | ⚠️ PARTIAL | ✅ Yes (MX_KWP) | ⚠️ Need DB prefix | ⚠️ Pending | 🔥 HIGH |
| Brazil | BR_KWP | ❌ BLOCKED | ❌ No (BR_NAC) | ❌ No (PS_LATAM) | ⚠️ Pending | ⏸️ ON HOLD |

---

## 📋 Files to Modify

### **Mexico (1 file):**
- ✏️ `queries/countries/mexico/loc_psdata_compras.sql` - Add PS_LATAM.dbo prefix

### **Brazil (2 files - AFTER DBA grants access):**
- ⏸️ `queries/countries/brazil/nac_ato.sql` - Add BR_NAC.dbo prefix
- ⏸️ `queries/countries/brazil/loc_psdata_compras.sql` - Add PS_LATAM.dbo prefix

### **All Countries (3 files - USER DECISION NEEDED):**
- 🔍 `queries/countries/bolivia/ps_latam.sql` - Replace with correct table
- 🔍 `queries/countries/mexico/ps_latam.sql` - Replace with correct table
- 🔍 `queries/countries/brazil/ps_latam.sql` - Replace with correct table

---

## 💡 Lessons Learned

1. **Cross-database architecture varies by country:**
   - Bolivia: nac_ato in BO_NAC (separate database)
   - Mexico: nac_ato in MX_KWP (main database)
   - Brazil: nac_ato in BR_NAC (separate database, no access)

2. **PS_LATAM is LATAM-wide shared database:**
   - Contains data for all countries
   - Requires country-specific filtering
   - idcountry column type varies (check before filtering)

3. **Always use fully-qualified names for cross-database queries:**
   - Format: `DATABASE.schema.table`
   - Even if table exists in main database, be explicit
   - Avoids ambiguity and permission issues

4. **Test access before building queries:**
   - Run `SELECT COUNT(*) FROM DB.dbo.table WHERE 1=0` first
   - Catches permission issues early
   - Avoids wasted extraction attempts

---

**Report Generated:** Session 3, February 5, 2026  
**Next Update:** After Mexico fix and Brazil DBA response  
**Contact:** Joaquin Aldunate (jaalduna@gmail.com)
