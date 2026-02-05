# 🚀 Quick Command Reference - Multi-Country Extraction

## 📊 Check Bolivia Extraction Status

```bash
# Monitor live
tail -f extraction_session3_restart.log

# Check last 30 lines
tail -30 extraction_session3_restart.log

# Count successful tables
grep "Created table" extraction_session3_restart.log | wc -l

# Check for our key tables
grep -E "(nac_ato|loc_psdata_compras)" extraction_session3_restart.log | grep "Created table"

# Check for errors
grep -i "error\|failed" extraction_session3_restart.log | tail -20
```

---

## 🇲🇽 Test Mexico Fix

```bash
# Test single table (loc_psdata_compras) - FAST TEST
poetry run sql-databricks-bridge extract \
  --queries-path queries \
  --country mexico \
  --query-name loc_psdata_compras \
  --destination 000-sql-databricks-bridge.mexico \
  --verbose

# Run full Mexico extraction (WARNING: 74M rows = 1-2 hours!)
poetry run sql-databricks-bridge extract \
  --queries-path queries \
  --country mexico \
  --destination 000-sql-databricks-bridge.mexico \
  --overwrite \
  --verbose > extraction_mexico.log 2>&1
```

---

## 🇧🇷 Brazil - After DBA Grants Access

```bash
# Test BR_NAC access
poetry run python << 'EOF'
from sql_databricks_bridge.db.sql_server import SQLServerClient
client = SQLServerClient(country='Brasil')
result = client.execute_query("SELECT COUNT(*) FROM BR_NAC.dbo.nac_ato")
print(f"nac_ato rows: {result['cnt'][0]:,}")
client.close()
EOF

# Test PS_LATAM access
poetry run python << 'EOF'
from sql_databricks_bridge.db.sql_server import SQLServerClient
client = SQLServerClient(country='Brasil')
result = client.execute_query("SELECT COUNT(*) FROM PS_LATAM.dbo.loc_psdata_compras")
print(f"loc_psdata_compras rows: {result['cnt'][0]:,}")
client.close()
EOF

# Run Brazil extraction
poetry run sql-databricks-bridge extract \
  --queries-path queries \
  --country brasil \
  --destination 000-sql-databricks-bridge.brazil \
  --overwrite \
  --verbose > extraction_brazil.log 2>&1
```

---

## 🔍 Investigate ps_latam Alternatives

```bash
# Check available psData tables
poetry run python << 'EOF'
from sql_databricks_bridge.db.sql_server import SQLServerClient
client = SQLServerClient(country='Bolivia')  # Or Mexico/Brasil

result = client.execute_query("""
    SELECT TABLE_NAME 
    FROM PS_LATAM.INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME LIKE 'psData%'
    ORDER BY TABLE_NAME
""")

print("Available psData tables:")
for i in range(len(result)):
    print(f"  - {result['TABLE_NAME'][i]}")

client.close()
EOF

# Check table schema
poetry run python << 'EOF'
from sql_databricks_bridge.db.sql_server import SQLServerClient
client = SQLServerClient(country='Bolivia')

table_name = "psData"  # Or psDataBolivia, etc.

result = client.execute_query(f"""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    FROM PS_LATAM.INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = '{table_name}'
    ORDER BY ORDINAL_POSITION
""")

print(f"\nSchema for {table_name}:")
for i in range(len(result)):
    dtype = result['DATA_TYPE'][i]
    max_len = result['CHARACTER_MAXIMUM_LENGTH'][i] or ''
    print(f"  {result['COLUMN_NAME'][i]:30} {dtype}{f'({max_len})' if max_len else ''}")

client.close()
EOF

# Check sample data
poetry run python << 'EOF'
from sql_databricks_bridge.db.sql_server import SQLServerClient
client = SQLServerClient(country='Bolivia')

result = client.execute_query("SELECT TOP 5 * FROM PS_LATAM.dbo.psData")
print(f"\nColumns: {', '.join(result.columns)}")
print(f"Sample rows: {len(result)}")
print(result)

client.close()
EOF
```

---

## 📊 Verify Data in Databricks

```sql
-- Run in Databricks SQL

-- Bolivia: Check nac_ato
SELECT 
    COUNT(*) as row_count,
    MIN(datacoleta) as first_date,
    MAX(datacoleta) as last_date,
    COUNT(DISTINCT codigopanelista) as panelists
FROM `000-sql-databricks-bridge`.`bolivia`.`nac_ato`;

-- Bolivia: Check loc_psdata_compras
SELECT 
    COUNT(*) as row_count,
    MIN(feviaje) as first_date,
    MAX(feviaje) as last_date,
    COUNT(DISTINCT entryid_viagem) as trips
FROM `000-sql-databricks-bridge`.`bolivia`.`loc_psdata_compras`;

-- List all Bolivia tables
SELECT table_name, table_type
FROM `000-sql-databricks-bridge`.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'bolivia'
ORDER BY table_name;

-- Check row counts for all tables
SELECT 
    table_name,
    'Run COUNT(*) manually' as note
FROM `000-sql-databricks-bridge`.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'bolivia'
ORDER BY table_name;
```

---

## 🔧 Database Access Check

```bash
# List available databases
poetry run python << 'EOF'
from sql_databricks_bridge.db.sql_server import SQLServerClient

for country in ['Bolivia', 'Mexico', 'Brasil']:
    print(f"\n{'='*50}")
    print(f"{country} - Available Databases")
    print('='*50)
    
    client = SQLServerClient(country=country)
    result = client.execute_query("SELECT name FROM sys.databases ORDER BY name")
    
    for i in range(len(result)):
        print(f"  {result['name'][i]}")
    
    client.close()
EOF

# Test specific database access
poetry run python << 'EOF'
from sql_databricks_bridge.db.sql_server import SQLServerClient

client = SQLServerClient(country='Bolivia')

# Test access to specific database
try:
    result = client.execute_query("SELECT COUNT(*) FROM BO_NAC.dbo.nac_ato WHERE 1=0")
    print("✅ Access granted to BO_NAC.dbo.nac_ato")
except Exception as e:
    print(f"❌ Access denied: {str(e)[:100]}")

client.close()
EOF
```

---

## 📝 Git Commands

```bash
# Check modified files
git status

# See changes
git diff queries/countries/bolivia/nac_ato.sql
git diff queries/countries/mexico/loc_psdata_compras.sql

# Stage changes
git add queries/countries/bolivia/nac_ato.sql
git add queries/countries/bolivia/loc_psdata_compras.sql
git add queries/countries/mexico/loc_psdata_compras.sql
git add DBA_ACCESS_REQUEST_BRAZIL.md
git add CROSS_COUNTRY_VALIDATION_REPORT.md
git add MULTI_COUNTRY_VALIDATION_SUMMARY.md

# Commit
git commit -m "Fix cross-database queries for Bolivia and Mexico; document Brazil access needs"

# Push
git push origin master
```

---

## 🧹 Cleanup Old Logs

```bash
# List log files
ls -lh *.log

# Archive old logs
mkdir -p logs_archive
mv bolivia_extraction_*.log logs_archive/
mv test_fixed_queries.log logs_archive/

# Keep only current logs:
# - extraction_session3_restart.log (current Bolivia run)
# - newly_extracted_tables.log (previous attempt)
```

---

## 💾 Backup Before Major Operations

```bash
# Before running full Mexico extraction (74M rows!)
cp -r queries/countries/mexico queries/countries/mexico.backup

# Before modifying Brazil queries
cp -r queries/countries/brazil queries/countries/brazil.backup

# Backup all countries
tar -czf queries_backup_$(date +%Y%m%d_%H%M%S).tar.gz queries/
```

---

## 🎯 Quick Health Check - All Countries

```bash
# Count query files
echo "Query file counts:"
for country in bolivia mexico brazil; do
  count=$(ls queries/countries/$country/*.sql 2>/dev/null | wc -l)
  echo "  $country: $count files"
done

# Check for cross-database references
echo -e "\nCross-database queries:"
grep -h "from.*\." queries/countries/*/*.sql | grep -v "^--" | grep -v "^\s*$" | sort -u

# Check for ps_latam references
echo -e "\nps_latam queries (need fixing):"
grep -l "from ps_latam" queries/countries/*/*.sql
```

---

## 📊 Extract Single Table (Fast Test)

```bash
# Bolivia - test single table
poetry run sql-databricks-bridge extract \
  --queries-path queries \
  --country bolivia \
  --query-name nac_ato \
  --destination 000-sql-databricks-bridge.bolivia \
  --verbose

# Mexico - test single table
poetry run sql-databricks-bridge extract \
  --queries-path queries \
  --country mexico \
  --query-name loc_psdata_compras \
  --destination 000-sql-databricks-bridge.mexico \
  --verbose
```

---

## 🚨 Emergency Stop

```bash
# If extraction is hanging or needs to be stopped
ps aux | grep sql-databricks-bridge
kill <PID>

# Or more forceful
pkill -9 -f sql-databricks-bridge
```

---

## ✅ Session Complete Checklist

```bash
# 1. Bolivia extraction finished?
tail -30 extraction_session3_restart.log | grep -i "complete\|done"

# 2. Count successful Bolivia tables
grep "Created table.*bolivia" extraction_session3_restart.log | wc -l

# 3. Verify in Databricks (run SQL from "Verify Data in Databricks" section)

# 4. Git commit changes
git status
git add -A
git commit -m "Session 3: Multi-country validation and fixes"
git push

# 5. Document decisions
# - ps_latam replacement chosen?
# - Brazil DBA request submitted?
# - Mexico extraction scheduled?
```

---

**Quick Reference Created:** Session 3, February 5, 2026  
**Keep this handy for:** Daily operations, testing, troubleshooting  
**Update as needed:** When adding new countries or queries
