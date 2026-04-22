-- KTCLSQL002 (Bolivia, etc.): loc_psdata_compras
-- Append-only. `id` is the monotonically-increasing PK used as watermark.
-- Sync is driven by MAX(id) from the Databricks target — no lookback filter
-- needed here so SQL Server can seek directly by the id index.

SELECT
    id,
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
    wt,
    CONVERT(VARCHAR(6), feviaje, 112) AS periodo
FROM [PS_LATAM].dbo.loc_psdata_compras
