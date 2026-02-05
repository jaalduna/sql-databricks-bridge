# 🔍 PS_LATAM Table Investigation

## Problem
The SQL query `ps_latam.sql` references a table that **does NOT exist** in the PS_LATAM database.

```sql
-- Current query (BROKEN):
from PS_LATAM.dbo.ps_latam  -- ❌ Table doesn't exist!
```

## Available Tables in PS_LATAM Database (131 tables)

### **Survey Data Tables (Most Likely Candidates):**

#### 1. **`psData`** - Main survey response data ⭐ **MOST LIKELY**
- Primary survey data table
- Contains actual survey responses/data
- Links to survey definitions

#### 2. **`psGenSurvey`** - Survey generation/definitions
- Survey structure and configuration
- Template definitions

#### 3. **`psSurvey`** - Survey metadata
- Survey instances and versions
- Survey timing/scheduling info

#### 4. **`psAnswers`** - Survey answers
- Individual answer records
- Response data

#### 5. **`psQuestions`** - Survey questions
- Question bank
- Question definitions

### **Other Available Tables:**
```
loc_psdata_compras          ✅ Already using this (13.1M rows)
psData                      ⭐ Most likely alternative
psGenSurvey                 Survey definitions
psSurvey                    Survey metadata
psAnswers                   Answer records
psQuestions                 Question records
psDataPalestina             Palestine survey data
psDataAmerica               Americas survey data
psDataChile                 Chile survey data
psDataBolivia               ⭐ Bolivia-specific survey data
psDataLatinAmerica          LATAM-wide survey data
```

### **Deprecated Tables (DO NOT USE):**
- `_NO_USAR_*` (24 tables) - Marked as deprecated
- Various archive/backup tables

## 🎯 DECISION NEEDED FROM USER

**Question:** What data were you expecting from the `ps_latam` query?

### **Option A: General Survey Data**
Replace with: **`psData`**
```sql
select *
from PS_LATAM.dbo.psData
```

### **Option B: Bolivia-Specific Survey Data** ⭐ **RECOMMENDED**
Replace with: **`psDataBolivia`**
```sql
select *
from PS_LATAM.dbo.psDataBolivia
where [country/date filters]
```

### **Option C: LATAM-Wide Survey Data**
Replace with: **`psDataLatinAmerica`**
```sql
select *
from PS_LATAM.dbo.psDataLatinAmerica
where CountryCode = 'BO'  -- or similar filter
```

### **Option D: Survey Definitions**
Replace with: **`psGenSurvey`** or **`psSurvey`**
```sql
select *
from PS_LATAM.dbo.psGenSurvey
```

## 📊 How to Investigate

Run this query to explore available data:

```python
poetry run python << 'EOF'
from sql_databricks_bridge.db.sql_server import SQLServerClient

client = SQLServerClient(country='bolivia')

# Check psData structure
print("="*60)
print("SCHEMA: psData")
print("="*60)
result = client.execute_query("""
    SELECT TOP 5 * FROM PS_LATAM.dbo.psData
""")
print(f"Columns: {', '.join(result.columns)}")
print(f"Sample rows: {len(result)}")

# Check psDataBolivia if it exists
print("\n" + "="*60)
print("SCHEMA: psDataBolivia")
print("="*60)
try:
    result = client.execute_query("""
        SELECT TOP 5 * FROM PS_LATAM.dbo.psDataBolivia
    """)
    print(f"Columns: {', '.join(result.columns)}")
    print(f"Sample rows: {len(result)}")
except:
    print("Table psDataBolivia doesn't exist or is empty")

client.close()
EOF
```

## ✅ ACTION ITEMS

1. **User decides** which table to use (A, B, C, or D above)
2. Update `queries/countries/bolivia/ps_latam.sql` with correct table name
3. Re-run extraction for that single table:
   ```bash
   poetry run sql-databricks-bridge extract \
     --queries-path queries \
     --country bolivia \
     --destination 000-sql-databricks-bridge.bolivia \
     --query-name ps_latam \
     --overwrite \
     --verbose
   ```

## 💡 RECOMMENDATION

Based on the query name `ps_latam` and Bolivia context, I recommend:

**Try Option B first: `psDataBolivia`** (Bolivia-specific survey data)

This is most likely what was intended, as it:
- ✅ Lives in PS_LATAM database (matches intent)
- ✅ Bolivia-specific (matches country)
- ✅ Follows naming pattern of other psData* tables
- ✅ Likely contains the survey/panel data for Bolivia specifically

If that doesn't have the expected data, fall back to **`psData`** (general survey table).

---

**Status:** ⏸️ Waiting for user decision on correct table name
**Priority:** MEDIUM (1 of 7 remaining failures)
**Impact:** Could unlock additional survey/behavioral data for Bolivia
