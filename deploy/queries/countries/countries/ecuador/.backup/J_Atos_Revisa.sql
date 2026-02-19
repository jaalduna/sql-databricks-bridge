-- =============================================================================
-- extraction: purchase acts for review (j_atoscompra_new - review subset)
-- source table: ecu_kwp.dbo.j_atoscompra_new
-- target table: 003-precios.bronze-data.fact_atos_revisa
-- parameters: 202401, 202412
-- =============================================================================

select
    j.idato,
    j.idartigo,
    j.periodo,
    j.coef_01,
    j.coef_02,
    j.coef_03,
    j.coef_01_pm,
    j.coef_02_pm,
    j.coef_03_pm,
    j.idpainel,
    j.preco,
    j.value_pm,
    j.forma_compra,
    j.quantidade,
    j.pack_comprado,
    j.unidades_packs,
    j.idcanal,
    j.idpromocao,
    j.flgregalo

from j_atoscompra_new

where j.periodo between 202401 and 202412
