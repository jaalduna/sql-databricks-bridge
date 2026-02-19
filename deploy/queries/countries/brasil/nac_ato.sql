-- brasil: nac_ato
-- columns: 14
-- Source: nac_ato database (cross-database query)
-- STATUS: DISABLED - Database 'nac_ato' not found on KTCLSQL003
-- TODO: Verify correct database name with DBA
-- Possible alternatives:
--   - Different server (not KTCLSQL003)
--   - Different database name (BR_NAC, BR_nac_ato, etc.)
--   - Database may not exist or requires special permissions

-- COMMENTED OUT UNTIL DATABASE LOCATION IS VERIFIED:
-- select
--     coef01,
--     coef02,
--     coef03,
--     datacoleta,
--     duplicado,
--     frmcompra,
--     idato,
--     numpack,
--     preco,
--     qtde,
--     unipack,
--     ventasuelta,
--     volume,
--     vsfrmcompra
-- from nac_ato.dbo.nac_ato

-- Temporary workaround: Return empty result set
SELECT 
    CAST(NULL AS FLOAT) AS coef01,
    CAST(NULL AS FLOAT) AS coef02,
    CAST(NULL AS FLOAT) AS coef03,
    CAST(NULL AS DATETIME) AS datacoleta,
    CAST(NULL AS VARCHAR(50)) AS duplicado,
    CAST(NULL AS VARCHAR(50)) AS frmcompra,
    CAST(NULL AS BIGINT) AS idato,
    CAST(NULL AS FLOAT) AS numpack,
    CAST(NULL AS FLOAT) AS preco,
    CAST(NULL AS FLOAT) AS qtde,
    CAST(NULL AS VARCHAR(50)) AS unipack,
    CAST(NULL AS VARCHAR(50)) AS ventasuelta,
    CAST(NULL AS FLOAT) AS volume,
    CAST(NULL AS VARCHAR(50)) AS vsfrmcompra
WHERE 1=0  -- Returns empty result with schema
