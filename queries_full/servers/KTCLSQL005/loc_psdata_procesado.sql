-- KTCLSQL005: loc_psdata_procesado
-- Append-only. FeDataCopy is the PK / monotonically-increasing insertion timestamp.
-- Sync is driven by MAX(FeDataCopy) from the Databricks target — no lookback filter
-- needed here so SQL Server can seek directly by the FeDataCopy index.
SELECT *, CONVERT(VARCHAR(6), FeProcesado, 112) AS periodo
FROM [PS_LATAM].dbo.loc_psdata_procesado
