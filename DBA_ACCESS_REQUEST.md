# DATABASE ACCESS REQUEST FOR BOLIVIA EXTRACTION

**Date:** February 5, 2026 (UPDATED)  
**Requestor:** KT\70088287 (Windows Authentication)  
**Purpose:** Bolivia Data Extraction to Databricks

---

## EXECUTIVE SUMMARY

During the Bolivia data extraction process, we identified that **3 tables** require access to **2 additional databases** (`NAC_ATO` and `PS_LATAM`) that the current user account cannot access.

**Current Status:**
- ✅ 30 of 39 tables successfully extracted (77%)
- ❌ 3 tables blocked by database permissions
- ❌ 6 tables have other issues (schema mismatches or don't exist)

---

## DATABASE ACCESS REQUESTS

### **REQUEST #1: NAC_ATO Database**

**Database Details:**
- **Database Name:** `NAC_ATO`
- **Server:** `KTCLSQL002.KT.group.local`
- **Instance:** Default SQL Server instance

**User Account:**
- **Login:** `KT\70088287` (Windows Authentication)
- **Current Access:** BO_KWP database (db_datareader)
- **Requested Access:** NAC_ATO database (db_datareader)

**Permission Level Needed:**
- **Role:** `db_datareader` (read-only access)
- **Scope:** All tables/views in NAC_ATO database

**Table to Access:**
- `NAC_ATO.dbo.nac_ato` - Transaction detail data

**Error Message Currently Received:**
```
[08004] [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]
The server principal "KT\70088287" is not able to access the database 
"NAC_ATO" under the current security context. (916)
```

---

### **REQUEST #2: PS_LATAM Database**

**Database Details:**
- **Database Name:** `PS_LATAM`
- **Server:** `KTCLSQL002.KT.group.local`
- **Instance:** Default SQL Server instance

**User Account:**
- **Login:** `KT\70088287` (Windows Authentication)
- **Current Access:** BO_KWP database (db_datareader)
- **Requested Access:** PS_LATAM database (db_datareader)

**Permission Level Needed:**
- **Role:** `db_datareader` (read-only access)
- **Scope:** All tables/views in PS_LATAM database

**Tables to Access:**
- `PS_LATAM.dbo.loc_psdata_compras` - Purchase trip/journey data
- `PS_LATAM.dbo.ps_latam` - LATAM regional data

**Error Message Currently Received:**
```
[42S02] [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]
Invalid object name 'loc_psdata_compras'. (208)
Invalid object name 'ps_latam'. (208)
```

---

## BUSINESS JUSTIFICATION

### **What Data is Blocked:**

**1. NAC_ATO.dbo.nac_ato**
- **Expected Rows:** ~9.6 million transaction detail records
- **Columns:** 14 (coef01-03, datacoleta, duplicado, frmcompra, idato, numpack, preco, qtde, unipack, ventasuelta, volume, vsfrmcompra)
- **Business Use:** Most granular transaction-level data with product coefficients, pricing, and quantities

**2. PS_LATAM.dbo.loc_psdata_compras**
- **Expected Rows:** Unknown (substantial volume expected)
- **Columns:** 11 (entryid_ato, entryid_viagem, feviaje, flg_duplicado, formacompra, granel, idcountry, itemprice, itemqty, vol, wt)
- **Business Use:** Purchase data with trip/journey context, including travel dates and purchase forms

**3. PS_LATAM.dbo.ps_latam**
- **Expected Rows:** Unknown
- **Columns:** All columns (using SELECT *)
- **Business Use:** LATAM-wide regional reference data

### **Impact of Not Having Access:**

**Current Workaround:**
- We have successfully extracted `j_atoscompra_new` (1.6M transactions)
- We have successfully extracted `hato_cabecalho` (1.3M purchase headers)
- These provide transaction data but at a higher aggregation level

**Value of Getting Access:**
- `nac_ato` would provide the most granular transaction details with product-level coefficients
- `loc_psdata_compras` would add trip/journey context to purchases
- `ps_latam` would provide regional reference data for cross-country analysis
- Would enable more detailed basket analysis, product attribution, and journey analytics

**Business Priority:** MEDIUM
- Current extraction is production-ready without these tables (77% success)
- Would significantly enhance analysis capabilities if granted
- Unlocks ~10M additional transaction detail rows

---

## TECHNICAL DETAILS

### **Database Structure:**

The Bolivia data is spread across **three databases** on the same server:

1. **BO_KWP** ✅ (Current Access)
   - Contains 30 successfully extracted tables
   - Dimension tables (demographics, products, channels)
   - Aggregated transaction tables

2. **NAC_ATO** ❌ (Need Access)
   - Contains detailed transaction-level data
   - Granular product and pricing information

3. **PS_LATAM** ❌ (Need Access)
   - LATAM-wide regional database
   - Cross-country reference data
   - Trip/journey-level purchase data

### **Why Tables Weren't Found:**

Our queries initially tried to access:
```sql
-- This fails because tables are in different databases:
SELECT * FROM nac_ato                    -- Should be: NAC_ATO.dbo.nac_ato
SELECT * FROM loc_psdata_compras         -- Should be: PS_LATAM.dbo.loc_psdata_compras
SELECT * FROM ps_latam                   -- Should be: PS_LATAM.dbo.ps_latam
```

We've now updated our queries to use fully qualified database names, but still need access permissions.

---

## SQL COMMANDS FOR DBA

The DBA can grant access with these commands:

### **Grant Access to NAC_ATO:**
```sql
-- On KTCLSQL002.KT.group.local SQL Server
USE [NAC_ATO];
GO

-- Add user to database if not already present
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'KT\70088287')
BEGIN
    CREATE USER [KT\70088287] FOR LOGIN [KT\70088287];
END
GO

-- Grant read-only access
ALTER ROLE db_datareader ADD MEMBER [KT\70088287];
GO

-- Verify access granted
SELECT 
    dp.name as username,
    r.name as role_name
FROM sys.database_principals dp
INNER JOIN sys.database_role_members drm ON dp.principal_id = drm.member_principal_id
INNER JOIN sys.database_principals r ON drm.role_principal_id = r.principal_id
WHERE dp.name = 'KT\70088287';
GO
```

### **Grant Access to PS_LATAM:**
```sql
-- On KTCLSQL002.KT.group.local SQL Server
USE [PS_LATAM];
GO

-- Add user to database if not already present
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'KT\70088287')
BEGIN
    CREATE USER [KT\70088287] FOR LOGIN [KT\70088287];
END
GO

-- Grant read-only access
ALTER ROLE db_datareader ADD MEMBER [KT\70088287];
GO

-- Verify access granted
SELECT 
    dp.name as username,
    r.name as role_name
FROM sys.database_principals dp
INNER JOIN sys.database_role_members drm ON dp.principal_id = drm.member_principal_id
INNER JOIN sys.database_principals r ON drm.role_principal_id = r.principal_id
WHERE dp.name = 'KT\70088287';
GO
```

---

## SUMMARY OF ALL FAILED TABLES

For context, here are all 9 failed extractions and their reasons:

| # | Table Name | Reason | Can DBA Fix? | Database |
|---|------------|--------|--------------|----------|
| 1 | `nac_ato` | **Permission denied** | **YES - REQUEST #1** | NAC_ATO |
| 2 | `loc_psdata_compras` | **Permission denied** | **YES - REQUEST #2** | PS_LATAM |
| 3 | `ps_latam` | **Permission denied** | **YES - REQUEST #2** | PS_LATAM |
| 4 | `sector` | Table doesn't exist | No (use a_sector ✅) | BO_KWP |
| 5 | `tblproductosinternos` | Table doesn't exist | No (not found) | BO_KWP |
| 6 | `paineis` | Column schema mismatch | No (query needs fixing) | BO_KWP |
| 7 | `pre_mordom` | Column schema mismatch | No (query needs fixing) | BO_KWP |
| 8 | `rg_domicilios_pesos` | Column schema mismatch | No (query needs fixing) | BO_KWP |
| 9 | `rg_panelis` | Column schema mismatch | No (query needs fixing) | BO_KWP |

**3 of 9 failures can be resolved by granting database access to NAC_ATO and PS_LATAM.**

---

## TESTING AFTER ACCESS IS GRANTED

Once access is granted, we can verify with:

```sql
-- Test NAC_ATO access
SELECT COUNT(*) FROM NAC_ATO.INFORMATION_SCHEMA.TABLES;
SELECT COUNT(*) FROM NAC_ATO.dbo.nac_ato;

-- Test PS_LATAM access
SELECT COUNT(*) FROM PS_LATAM.INFORMATION_SCHEMA.TABLES;
SELECT COUNT(*) FROM PS_LATAM.dbo.loc_psdata_compras;
SELECT COUNT(*) FROM PS_LATAM.dbo.ps_latam;

-- If successful, we'll see row counts instead of permission errors
```

From our data extraction tool:
```bash
# Re-run extraction for the 3 newly accessible tables
poetry run sql-databricks-bridge extract \
  --queries-path queries \
  --country bolivia \
  --destination 000-sql-databricks-bridge.bolivia \
  --overwrite \
  --verbose

# Expected results:
# OK nac_ato: ~9,600,000 rows
# OK loc_psdata_compras: [substantial rows]
# OK ps_latam: [rows from LATAM regional table]
```

---

## CONTACT INFORMATION

**Requestor:**
- Name: Joaquin Aldunate
- Email: jaalduna@gmail.com
- User Account: KT\70088287

**Purpose:**
- Project: SQL to Databricks Data Bridge
- Use Case: Bolivia Market Data Extraction
- Data Destination: Databricks Workspace (000-sql-databricks-bridge catalog)

**Questions?**
Contact the requestor or refer to project documentation in:
`C:\Users\70088287\Documents\Projects\sql-databricks-bridge`

---

## APPENDIX: WHAT'S WORKING

For context, we have successfully extracted from **BO_KWP**:

**Transaction Data:**
- ✅ `j_atoscompra_new` - 1,607,596 transactions (main table)
- ✅ `hato_cabecalho` - 1,330,321 purchase headers

**Demographics:**
- ✅ `individuo` - 90,272 individuals
- ✅ `domicilios` - 5,842 households
- ✅ `paineis_individuos` - 23,912 panel members

**Product Data:**
- ✅ `a_marcaproduto` - 18,682 brands
- ✅ `a_fabricanteproduto` - 7,253 manufacturers
- ✅ `vw_artigoz_all` - 116,760 articles

**Channel Data:**
- ✅ `a_canal` - 114 channels
- ✅ `vw_venues` - 555 venues

**Total:** 30 tables, ~3.2 million rows successfully extracted from BO_KWP

---

## EMAIL TEMPLATE FOR REQUEST

```
Subject: Database Access Request - NAC_ATO & PS_LATAM (Read-Only) for Bolivia Data Extraction

Hi [DBA Name],

I'm working on extracting Bolivia market data from SQL Server to Databricks and need read-only access to two additional databases on KTCLSQL002.KT.group.local.

DATABASES NEEDED:
1. NAC_ATO - Transaction detail data (~9.6M rows)
2. PS_LATAM - Regional purchase and trip data

REQUEST DETAILS:
- Server: KTCLSQL002.KT.group.local
- Databases: NAC_ATO, PS_LATAM
- User: KT\70088287 (Windows Authentication)
- Permission Needed: db_datareader (read-only) for both databases

TABLES TO ACCESS:
- NAC_ATO.dbo.nac_ato (transaction details - 14 columns)
- PS_LATAM.dbo.loc_psdata_compras (purchase trip data - 11 columns)
- PS_LATAM.dbo.ps_latam (LATAM regional data)

CURRENT STATUS:
- BO_KWP database: ✅ Working (30/39 tables extracted, 77% success)
- NAC_ATO database: ❌ No access (permission denied error 08004/916)
- PS_LATAM database: ❌ No access (table not found error 42S02/208)

BUSINESS IMPACT:
- Would unlock 3 critical tables with ~10M transaction detail records
- Current extraction is functional but missing granular detail data
- Priority: MEDIUM (enhancement to enable deeper analytics)

SQL COMMANDS:
The SQL commands to grant access are in the attached document (DBA_ACCESS_REQUEST.md).

Thank you!
Joaquin Aldunate
KT\70088287
jaalduna@gmail.com
```

---

**Priority:** MEDIUM (system is production-ready without these, but would enhance capabilities)  
**Urgency:** LOW (no business deadline, enhancement only)  
**Risk:** NONE (read-only access request)  
**Databases Needed:** 2 (NAC_ATO, PS_LATAM)  
**Tables Unlocked:** 3 (nac_ato, loc_psdata_compras, ps_latam)

**Request Date:** February 5, 2026 (Updated)
