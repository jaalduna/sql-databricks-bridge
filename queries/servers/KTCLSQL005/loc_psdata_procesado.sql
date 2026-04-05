-- KTCLSQL005: loc_psdata_procesado
-- columns: 11
-- Source: PS_LATAM database (server-wide shared table)
-- Description: Processed purchase trip/journey data for all countries on KTCLSQL005
-- Target schema: 000-sql-databricks-bridge.KTCLSQL005

SELECT *, CONVERT(VARCHAR(6), FeProcesado, 112) AS periodo
FROM [PS_LATAM].dbo.loc_psdata_procesado
WHERE FeProcesado >= DATEADD(MONTH, -{lookback_months}, GETDATE())
