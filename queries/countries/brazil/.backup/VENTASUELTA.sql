-- =============================================================================
-- extraction: bulk sales conversion factors (ventasuelta)
-- source table: br_kwp.dbo.ventasuelta
-- target table: 003-precios.bronze-data.dim_ventasuelta
-- parameters: none (full dimension extract)
-- =============================================================================

select
    idsub,
    idproduto,
    conversion
from ventasuelta
