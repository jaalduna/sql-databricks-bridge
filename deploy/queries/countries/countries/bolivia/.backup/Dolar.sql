-- =============================================================================
-- extraction: exchange rates (dolar)
-- source table: bo_kwp.dbo.dolar
-- target table: 003-precios.bronze-data.dim_dolar
-- parameters: none (full dimension extract, filtered by year >= 2020)
-- =============================================================================

select
    periodo,
    ano,
    tipo,
    [idpais] = idpais,
    valor

from dolar
where ano >= 2020
