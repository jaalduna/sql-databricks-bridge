-- bolivia: j_atoscompra_new
-- columns: 27

select
    coef_01,
    coef_01_pm,
    coef_02,
    coef_02_pm,
    coef_03,
    coef_03_pm,
    data_compra,
    flg_scanner,
    flggranel,
    flgregalo,
    forma_compra,
    idartigo,
    idato,
    idcanal,
    iddomicilio,
    idfabricante,
    idhato_cabecalho,
    idmarca,
    idpainel,
    idproduto,
    idpromocao,
    pack_comprado,
    periodo,
    preco,
    quantidade,
    unidades_packs,
    value_pm
from j_atoscompra_new
where periodo >= {start_period} and periodo <= {end_period}
