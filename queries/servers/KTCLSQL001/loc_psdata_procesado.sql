-- KTCLSQL001 (Mexico): loc_psdata_procesado
-- columns: 11
-- Source: PS_LATAM database (server-wide shared table)
-- Description: Processed purchase trip/journey data for all countries on KTCLSQL001
-- Target schema: 000-sql-databricks-bridge.KTCLSQL001

SELECT *
FROM [PS_LATAM].dbo.loc_psdata_procesado
