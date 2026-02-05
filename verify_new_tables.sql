-- ========================================
-- VERIFICATION QUERIES FOR NEW BOLIVIA TABLES
-- ========================================
-- Run these in Databricks after extraction completes
-- to verify the two new cross-database tables

-- ========================================
-- 1. Verify nac_ato (from BO_NAC database)
-- ========================================
-- Expected: ~9,634,153 rows
-- Transaction details with coefficients

SELECT 
    'nac_ato' as table_name,
    COUNT(*) as row_count,
    MIN(datacoleta) as first_date,
    MAX(datacoleta) as last_date,
    COUNT(DISTINCT codigopanelista) as unique_panelists,
    COUNT(DISTINCT nomeato) as unique_transactions
FROM `000-sql-databricks-bridge`.`bolivia`.`nac_ato`;

-- Sample data check
SELECT TOP 10 
    datacoleta,
    codigopanelista,
    nomeato,
    coef01,
    coef02,
    coef03,
    preco,
    quantidade,
    volume
FROM `000-sql-databricks-bridge`.`bolivia`.`nac_ato`
ORDER BY datacoleta DESC;


-- ========================================
-- 2. Verify loc_psdata_compras (from PS_LATAM database)
-- ========================================
-- Expected: ~13,176,985 rows
-- Purchase trip/journey data

SELECT 
    'loc_psdata_compras' as table_name,
    COUNT(*) as row_count,
    MIN(feviaje) as first_date,
    MAX(feviaje) as last_date,
    COUNT(DISTINCT entryid_viagem) as unique_trips,
    COUNT(DISTINCT formacompra) as unique_purchase_forms,
    SUM(CASE WHEN flg_duplicado = 1 THEN 1 ELSE 0 END) as duplicate_count
FROM `000-sql-databricks-bridge`.`bolivia`.`loc_psdata_compras`;

-- Sample data check
SELECT TOP 10 
    feviaje,
    entryid_ato,
    entryid_viagem,
    formacompra,
    grproduto,
    precio,
    cantidad,
    peso,
    volumen,
    flg_duplicado
FROM `000-sql-databricks-bridge`.`bolivia`.`loc_psdata_compras`
ORDER BY feviaje DESC;


-- ========================================
-- 3. Combined Summary - All Tables Status
-- ========================================
SELECT 
    table_schema,
    table_name,
    'Check row count manually' as note
FROM `000-sql-databricks-bridge`.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'bolivia'
ORDER BY table_name;


-- ========================================
-- 4. Key Business Questions - New Data Unlocked
-- ========================================

-- Q1: How much transaction detail data do we have by month?
SELECT 
    DATE_TRUNC('month', datacoleta) as month,
    COUNT(*) as transactions,
    COUNT(DISTINCT codigopanelista) as panelists,
    SUM(quantidade * preco) as total_value
FROM `000-sql-databricks-bridge`.`bolivia`.`nac_ato`
GROUP BY DATE_TRUNC('month', datacoleta)
ORDER BY month DESC
LIMIT 12;

-- Q2: What purchase channels are captured in loc_psdata_compras?
SELECT 
    formacompra,
    COUNT(*) as trip_count,
    COUNT(DISTINCT entryid_viagem) as unique_trips,
    AVG(precio) as avg_price,
    SUM(cantidad) as total_quantity
FROM `000-sql-databricks-bridge`.`bolivia`.`loc_psdata_compras`
GROUP BY formacompra
ORDER BY trip_count DESC;

-- Q3: Data quality - duplicates in loc_psdata_compras
SELECT 
    flg_duplicado,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM `000-sql-databricks-bridge`.`bolivia`.`loc_psdata_compras`
GROUP BY flg_duplicado;


-- ========================================
-- NOTES:
-- ========================================
-- These two tables unlock 22.8 million additional rows:
--   - nac_ato: 9.6M rows (transaction-level detail)
--   - loc_psdata_compras: 13.1M rows (trip/journey purchases)
--
-- This represents the majority of Bolivia's transactional data!
-- Previous extraction: 3.2M rows across 30 tables
-- New extraction: ~26M rows across 32 tables (8x increase!)
