-- KTCLSQL002 (Bolivia, etc.): loc_psdata_compras
-- columns: 11
-- Source: PS_LATAM database (server-wide shared table)
-- Description: Purchase trip/journey data for all countries on KTCLSQL002
-- Target schema: 000-sql-databricks-bridge.KTCLSQL002

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
