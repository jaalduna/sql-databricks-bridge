-- =============================================================================
-- extraction: payment methods dimension (a_formapagto)
-- source table: pe_kwp.dbo.a_formapagto
-- target table: 003-precios.bronze-data.dim_forma_pagto
-- parameters: none (full dimension extract)
-- =============================================================================

select
    afp.idformapagto,
    afp.descricao,
    afp.flgdesativacao,
    afp.dtcriacao,
    afp.dtatualizacao,
    afp.sempreco

from a_formapagto afp
