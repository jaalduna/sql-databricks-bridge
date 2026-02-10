# 🇧🇷 DBA Access Request - Brazil Data Extraction

**Date:** February 5, 2026  
**Updated:** February 5, 2026  
**Requestor:** Joaquin Aldunate (jaalduna@gmail.com)  
**Team:** Kantar Analytics  
**Purpose:** Data extraction for analytics and reporting  
**Status:** ✅ **APPROVED AND GRANTED**

---

## 📋 Request Summary

~~Requesting~~ READ access to Brazil transaction and purchase data tables for data pipeline extraction to Databricks.

**Similar Access Already Granted:**
- ✅ Bolivia: User has access to BO_NAC and PS_LATAM (approved and working)
- ✅ Mexico: User has access to MX_KWP (nac_ato exists in main database)
- ✅ Brazil: User has access to BR_NAC and PS_LATAM (**GRANTED**)

---

## 🔐 Required Permissions ✅ GRANTED

### **1. BR_NAC Database - Transaction Details** ✅ GRANTED

**Database:** BR_NAC  
**Server:** KTCLSQL003.KT.group.local  
**Permission Requested:** `db_datareader` role  
**Status:** ✅ **GRANTED**

**Justification:**
- Need access to `nac_ato` table (transaction-level purchase data)
- Same table structure as Bolivia BO_NAC.dbo.nac_ato (already have access)
- Required for consistent LATAM-wide analytics
- Estimated size: Multi-million rows based on other countries

**Tables Needed:**
- `nac_ato` - Transaction details with coefficients and pricing

---

### **2. PS_LATAM Database - Purchase Journey Data** ✅ GRANTED

**Database:** PS_LATAM  
**Server:** KTCLSQL003.KT.group.local  
**Permission Requested:** SELECT permission on `loc_psdata_compras` table  
**Status:** ✅ **GRANTED**

**Justification:**
- Need access to purchase trip/journey data
- Bolivia already has access to this same table in PS_LATAM
- PS_LATAM is a shared LATAM database containing data for all countries
- User already has access to PS_LATAM from Bolivia server (KTCLSQL002)
- Just need permission to access the same table from Brazil server (KTCLSQL003)

**Tables Needed:**
- `loc_psdata_compras` - Purchase trip data (LATAM-wide table)
- `loc_psdata_procesado` - Processed purchase data

---

## 🎯 Use Case

**Project:** SQL-Databricks Bridge Data Pipeline  
**Goal:** Extract Brazil panel data to Databricks for analytics

**Data Flow:**
1. Extract data from SQL Server (Brazil databases)
2. Stage to Parquet files in Databricks Volumes
3. Load into Databricks Delta tables
4. Enable analytics and reporting in Databricks

**Current Status:**
- ✅ 36 other Brazil tables already extracting successfully from BR_KWP
- ❌ 2 key transaction tables blocked due to permissions:
  - `nac_ato` (millions of transaction records)
  - `loc_psdata_compras` (purchase journey data)

---

## 📊 Impact

**Without Access:**
- Missing critical transaction-level data for Brazil
- Cannot perform cross-country LATAM analytics
- Bolivia and Mexico data available, but Brazil incomplete
- Reporting gaps for Brazil market

**With Access:**
- Complete Brazil transaction data
- Consistent data structure across all LATAM countries
- Enable regional analytics and comparisons
- Estimated additional data: 20-50M transaction rows

---

## 🔒 Security & Access Control

**User Account:** KT\70088287 (Joaquin Aldunate)  
**Access Type:** READ ONLY (db_datareader)  
**Duration:** Ongoing (production data pipeline)  
**Data Handling:** Extracted to secure Databricks environment

**Precedent:**
- Same user already has identical access for Bolivia:
  - BO_NAC database: db_datareader role ✅
  - PS_LATAM database: READ access ✅
- No security incidents or issues with Bolivia access
- Same use case, same user, same data pipeline

---

## ✅ Approval Workflow

**Business Sponsor:** [To be filled]  
**Manager Approval:** [To be filled]  
**DBA Contact:** [To be filled]  

**Priority:** Medium  
**Expected Timeframe:** 1-2 weeks  

---

## 📝 Technical Details

### **Connection Information:**

**Server:** KTCLSQL003.KT.group.local  
**Authentication:** Windows Authentication (Trusted_Connection=yes)  
**User:** KT\70088287  
**Current Access:** BR_KWP database (working)

### **Required SQL Commands (for DBA):**

```sql
-- Grant BR_NAC read access
USE BR_NAC;
ALTER ROLE db_datareader ADD MEMBER [KT\70088287];

-- Grant PS_LATAM table-specific read access
USE PS_LATAM;
GRANT SELECT ON dbo.loc_psdata_compras TO [KT\70088287];

-- Verify permissions
EXECUTE AS USER = 'KT\70088287';
SELECT HAS_PERMS_BY_NAME('BR_NAC.dbo.nac_ato', 'OBJECT', 'SELECT') AS has_nac_ato_access;
SELECT HAS_PERMS_BY_NAME('PS_LATAM.dbo.loc_psdata_compras', 'OBJECT', 'SELECT') AS has_psdata_access;
REVERT;
```

---

## 🧪 Post-Approval Testing

After access is granted, we will:

1. **Test Connectivity:**
   ```python
   from sql_databricks_bridge.db.sql_server import SQLServerClient
   
   client = SQLServerClient(country='Brasil')
   
   # Test BR_NAC access
   result = client.execute_query("SELECT COUNT(*) FROM BR_NAC.dbo.nac_ato")
   print(f"nac_ato rows: {result['cnt'][0]:,}")
   
   # Test PS_LATAM access
   result = client.execute_query("SELECT COUNT(*) FROM PS_LATAM.dbo.loc_psdata_compras")
   print(f"loc_psdata_compras rows: {result['cnt'][0]:,}")
   ```

2. **Update Query Files:**
   - Fix `queries/countries/brazil/nac_ato.sql` with BR_NAC.dbo prefix
   - Fix `queries/countries/brazil/loc_psdata_compras.sql` with PS_LATAM.dbo prefix

3. **Run Test Extraction:**
   ```bash
   poetry run sql-databricks-bridge extract \
     --queries-path queries \
     --country brasil \
     --query-name nac_ato \
     --destination 000-sql-databricks-bridge.brazil \
     --verbose
   ```

4. **Verify Data Quality:**
   - Check row counts match expected values
   - Validate data structure and column types
   - Confirm date ranges and data completeness

---

## 📞 Contact Information

**Requestor:** Joaquin Aldunate  
**Email:** jaalduna@gmail.com  
**Department:** Kantar Analytics  
**Manager:** [To be filled]  

**For Questions:**
- Technical: jaalduna@gmail.com
- Business Case: [Manager contact]
- DBA Issues: [DBA team contact]

---

## 📎 Attachments

1. Cross-Country Validation Report (CROSS_COUNTRY_VALIDATION_REPORT.md)
2. Bolivia Access Precedent (DBA_ACCESS_REQUEST.md)
3. Project Documentation (README.md)

---

## ✅ Next Steps

1. **Submit this request** to DBA team
2. **Business approval** from manager
3. **DBA implementation** (grant permissions)
4. **Testing** by requestor
5. **Documentation** update
6. **Production use** enabled

---

**Request Status:** DRAFT - Ready for submission  
**Created:** February 5, 2026  
**Last Updated:** February 5, 2026  
**Version:** 1.0
