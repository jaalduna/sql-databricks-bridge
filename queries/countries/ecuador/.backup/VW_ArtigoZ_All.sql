-- =============================================================================
-- extraction: full product catalog (vw_artigoz_all)
-- source table: ecu_kwp.dbo.vw_artigoz_all
-- target table: 003-precios.bronze-data.dim_artigos_all
-- parameters: none (full dimension extract)
-- =============================================================================

select
    vwa.idartigo,
    vwa.idsector,
    vwa.sector,
    vwa.idproduto,
    vwa.produto,
    vwa.idsub,
    vwa.sub,
    vwa.idconteudo,
    vwa.conteudo,
    vwa.mwp_idpl,
    vwa.mwp_pl,
    vwa.idfabricante,
    vwa.fabricante,
    vwa.idmarca,
    vwa.marca,
    vwa.cdc01,
    vwa.clas01,
    vwa.cdc02,
    vwa.clas02,
    vwa.cdc03,
    vwa.clas03,
    vwa.cdc04,
    vwa.clas04,
    vwa.cdc05,
    vwa.clas05,
    vwa.flggranel,
    vwa.coef1,
    vwa.unicoef1,
    vwa.coef2,
    vwa.unicoef2,
    vwa.coef3,
    vwa.unicoef3,
    vwa.flgativo,
    vwa.codbar,
    vwa.codbar2,
    vwa.codbar3,
    vwa.flgpack,
    vwa.unidadespack,
    vwa.idartigoind,
    vwa.dtcriacao

from vw_artigoz_all vwa
