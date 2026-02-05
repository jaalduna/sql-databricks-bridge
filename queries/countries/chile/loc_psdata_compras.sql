-- chile: loc_psdata_compras
-- columns: 11
-- Source: PS_LATAM database (cross-database query)

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
