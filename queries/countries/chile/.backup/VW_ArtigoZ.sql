-- =============================================================================
-- extraction: product catalog (vw_artigoz)
-- source table: cl_kwp.dbo.vw_artigoz
-- target table: 003-precios.bronze-data.dim_artigos
-- parameters: none (full dimension extract)
-- =============================================================================

select
    vw.idartigo,
    vw.idproduto,
    vw.produto,
    vw.idsub,
    vw.sub,
    vw.idconteudo,
    vw.conteudo,
    vw.mwp_idpl,
    vw.mwp_pl,
    vw.idfabricante,
    vw.fabricante,
    vw.idmarca,
    vw.marca,
    vw.cdc01,
    vw.clas01,
    vw.cdc02,
    vw.clas02,
    vw.cdc03,
    vw.clas03,
    vw.cdc04,
    vw.clas04,
    vw.cdc05,
    vw.clas05,
    vw.flggranel,
    vw.coef1,
    vw.unicoef1,
    vw.coef2,
    vw.unicoef2,
    vw.coef3,
    vw.unicoef3,
    vw.flgativo,
    vw.codbar,
    vw.codbar2,
    vw.codbar3,
    vw.flgpack,
    vw.unidadespack,
    vw.idartigoind,
    vw.dtcriacao

from vw_artigoz vw
