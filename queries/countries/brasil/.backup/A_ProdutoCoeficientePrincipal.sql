-- =============================================================================
-- extraction: product coefficients (a_produtocoeficienteprincipal)
-- source table: br_kwp.dbo.a_produtocoeficienteprincipal
-- target table: 003-precios.bronze-data.dim_coeficiente_principal
-- parameters: none (full dimension extract)
-- =============================================================================

select
    apcp.idproduto,
    apcp.idsub,
    apcp.idcoeficiente,
    apcp.unidade

from a_produtocoeficienteprincipal apcp
