-- =============================================================================
-- extraction: scanner data (nac_ato)
-- source table: co_sinc.dbo.nac_ato
-- target table: 003-precios.bronze-data.fact_nac_ato
-- parameters: co_sinc, 2024-01-01, 2024-12-31
-- note: cross-database query — requires access to co_sinc
-- =============================================================================

select
    idato,
    volume,
    qtde,
    preco,
    frmcompra,
    unipack,
    numpack,
    coef01,
    coef02,
    coef03,
    ventasuelta,
    vsfrmcompra,
    datacoleta,
    duplicado
from co_sinc.dbo.nac_ato
where duplicado = 0
    and datacoleta >= convert(datetime, '2024-01-01', 120)
    and datacoleta < convert(datetime, '2024-12-31', 120)
