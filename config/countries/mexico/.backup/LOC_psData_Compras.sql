-- =============================================================================
-- extraction: panelsmart purchase data (loc_psdata_compras)
-- source table: ps_latam.dbo.loc_psdata_compras
-- target table: 003-precios.bronze-data.fact_loc_ps_compras
-- parameters: ps_latam, and ps.idcountry = 52, 2024-01-01, 2024-12-31
-- note: cross-database query — requires access to ps_latam
--   most countries: ps_database=ps_latam, ps_filter="and ps.idcountry = {id}"
--   chile exception: ps_database=[cl_ps], ps_filter="" (no idcountry filter)
-- =============================================================================

select
    ps.entryid_ato,
    ps.itemprice,
    ps.itemqty,
    ps.wt,
    ps.entryid_viagem,
    ps.granel,
    ps.vol,
    ps.formacompra,
    ps.feviaje,
    ps.flg_duplicado,
    ps.idcountry
from ps_latam.dbo.loc_psdata_compras ps
where ps.flg_duplicado = 0
    and ps.idcountry = 52
    and ps.feviaje >= convert(datetime, '2024-01-01', 120)
    and ps.feviaje < convert(datetime, '2024-12-31', 120)
