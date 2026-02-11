-- =============================================================================
-- extraction: kit/pack product mapping (kitpack_tblproductosinternos)
-- source table: br_kwp.[kitpack].[tblproductosinternos]
-- target table: 003-precios.bronze-data.dim_kitpack
-- parameters: none (full dimension extract)
-- =============================================================================

select
    kptpi.codbarraext,
    kptpi.idartigo,
    kptpi.codbarraint,
    kptpi.unidad,
    kptpi.lasttransactionuser,
    kptpi.lasttransactiondate

from [kitpack].[tblproductosinternos] kptpi
