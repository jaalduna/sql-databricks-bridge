-- KTCLSQL001 (Mexico): loc_psdata_compras
-- columns: 11
-- Source: PS_LATAM database (server-wide shared table)
-- Description: Purchase trip/journey data for all countries on KTCLSQL001
-- Target schema: 000-sql-databricks-bridge.KTCLSQL001

SELECT
    entryid_ato,
    entryid_viagem,
    feviaje,
    flg_duplicado,
    formacompra,
    granel,
    idcountry,
    itemprice,
    itemqty,
    vol,
    wt
FROM [PS_LATAM].dbo.loc_psdata_compras
