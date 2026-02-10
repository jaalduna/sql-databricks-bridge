# 🚀 Quick Start: Server-Level PS_LATAM Extraction

## What Changed?
PS_LATAM queries moved from `queries/countries/*` to `queries/servers/KTCLSQL00X/`

## Why?
- PS_LATAM is **shared per server**, not per country
- Avoid extracting the same data multiple times
- Cleaner Databricks schema organization

---

## 📂 New Structure

```
queries/servers/
├── KTCLSQL001/  (Mexico server)
│   ├── loc_psdata_compras.sql
│   └── loc_psdata_procesado.sql
├── KTCLSQL002/  (Bolivia server)
│   ├── loc_psdata_compras.sql
│   └── loc_psdata_procesado.sql
├── KTCLSQL003/  (Brazil server)
│   ├── loc_psdata_compras.sql
│   └── loc_psdata_procesado.sql
├── KTCLSQL004/
│   ├── loc_psdata_compras.sql
│   └── loc_psdata_procesado.sql
└── KTCLSQL005/
    ├── loc_psdata_compras.sql
    └── loc_psdata_procesado.sql
```

---

## 🎯 Databricks Destination

```
000-sql-databricks-bridge (catalog)
├── KTCLSQL001 (schema)      ← Server-level tables
│   ├── loc_psdata_compras
│   └── loc_psdata_procesado
├── KTCLSQL002 (schema)
│   ├── loc_psdata_compras
│   └── loc_psdata_procesado
└── ...

├── bolivia (schema)          ← Country-specific tables
│   ├── hato_cabecalho
│   ├── nac_ato
│   └── ...
```

---

## ⚡ Quick Commands

### Extract PS_LATAM from KTCLSQL002 (Bolivia Server)

```bash
# loc_psdata_compras
poetry run sql-databricks-bridge extract \
  --queries-path queries/servers/KTCLSQL002 \
  --server KTCLSQL002.KT.group.local \
  --database PS_LATAM \
  --query-name loc_psdata_compras \
  --destination 000-sql-databricks-bridge.KTCLSQL002 \
  --verbose

# loc_psdata_procesado
poetry run sql-databricks-bridge extract \
  --queries-path queries/servers/KTCLSQL002 \
  --server KTCLSQL002.KT.group.local \
  --database PS_LATAM \
  --query-name loc_psdata_procesado \
  --destination 000-sql-databricks-bridge.KTCLSQL002 \
  --verbose
```

### Extract from All Servers (Batch)

```bash
#!/bin/bash
for i in {1..5}; do
  SERVER="KTCLSQL00${i}.KT.group.local"
  SCHEMA="KTCLSQL00${i}"
  
  echo "🔄 Extracting from ${SERVER}..."
  
  poetry run sql-databricks-bridge extract \
    --queries-path "queries/servers/KTCLSQL00${i}" \
    --server "${SERVER}" \
    --database "PS_LATAM" \
    --destination "000-sql-databricks-bridge.${SCHEMA}" \
    --verbose
done
```

---

## 📊 Query in Databricks

### Access Bolivia's PS_LATAM Data

```sql
-- All data from KTCLSQL002 (includes all countries on that server)
SELECT * FROM `000-sql-databricks-bridge`.KTCLSQL002.loc_psdata_compras;

-- Filter for Bolivia (idcountry value TBD - check with DBA)
SELECT * FROM `000-sql-databricks-bridge`.KTCLSQL002.loc_psdata_compras
WHERE idcountry = 1;  -- Assuming 1 = Bolivia

-- Join with country-specific table
SELECT h.*, p.*
FROM `000-sql-databricks-bridge`.bolivia.hato_cabecalho h
JOIN `000-sql-databricks-bridge`.KTCLSQL002.loc_psdata_compras p
  ON h.entryid_ato = p.entryid_ato;
```

---

## 🗺️ Server Mapping

| Server | Countries | Status |
|--------|-----------|--------|
| KTCLSQL001 | Mexico | ✅ Ready |
| KTCLSQL002 | Bolivia, ... | ✅ Ready |
| KTCLSQL003 | Brazil, ... | ⚠️ Need DB access |
| KTCLSQL004 | TBD | ✅ Ready |
| KTCLSQL005 | TBD | ✅ Ready |

---

## 📖 Full Documentation

See `SERVER_LEVEL_REFACTOR.md` for complete details.

---

## ✅ Next Steps

1. ✅ Server-level queries created
2. 🔄 **Run first extraction** (KTCLSQL002)
3. 🔄 **Verify schema in Databricks**
4. 🔄 Determine `idcountry` mapping
5. 🔄 Extract remaining servers (KTCLSQL001, 003, 004, 005)
6. 🔄 Update downstream analytics to use server-level tables
