-- brazil: loc_psdata_compras
-- columns: 24
-- Source: PS_LATAM database (cross-database query)

select
    codbarraext,
    codbarraint,
    coef01,
    coef02,
    coef03,
    datacoleta,
    estado,
    frmcompra,
    idartigo,
    idato,
    iddomicilio,
    idvenuebarcode,
    mes,
    numpack,
    preco,
    quantidade,
    regiao,
    unipack,
    ventasuelta,
    volume,
    vsfrmcompra,
    ano,
    mes_ano,
    pais
from PS_LATAM.dbo.loc_psdata_compras
