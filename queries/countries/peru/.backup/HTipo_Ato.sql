-- =============================================================================
-- extraction: purchase type dimension (htipo_ato)
-- source table: pe_kwp.dbo.htipo_ato
-- target table: 003-precios.bronze-data.dim_htipo_ato
-- parameters: none (full dimension extract)
-- =============================================================================

select
    tipo_ato,
    considerar,
    descricao
from htipo_ato
