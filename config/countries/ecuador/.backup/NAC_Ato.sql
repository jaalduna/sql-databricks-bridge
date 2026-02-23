-- =============================================================================
-- extraction: scanner data (nac_ato)
-- source table: ecu_sinc.dbo.nac_ato
-- target table: 003-precios.bronze-data.fact_nac_ato
-- parameters: ecu_sinc, 2024-01-01, 2024-12-31
-- note: cross-database query — requires access to ecu_sinc
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
from ecu_sinc.dbo.nac_ato
where duplicado = 0
    and datacoleta >= convert(datetime, '2024-01-01', 120)
    and datacoleta < convert(datetime, '2024-12-31', 120)
