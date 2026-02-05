-- mexico: loc_psdata_compras
-- columns: 11
-- database: PS_LATAM (cross-database query - shared LATAM table)
-- note: idcountry is INT type (not string)

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
