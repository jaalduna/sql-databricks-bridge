-- bolivia: loc_psdata_compras
-- columns: 11
-- database: PS_LATAM (requires db_datareader access)

select
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
from PS_LATAM.dbo.loc_psdata_compras
