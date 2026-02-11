-- =============================================================================
-- extraction: date/period dimension (dt_mes)
-- source table: cl_kwp.dbo.dt_mes
-- target table: 003-precios.bronze-data.dim_mes
-- parameters: none (full dimension extract, filtered by year >= 2020)
-- =============================================================================

select
    dtm.mes,
    dtm.año,
    dtm.desde,
    dtm.hasta

from dt_mes dtm
where dtm.año >= 2020
