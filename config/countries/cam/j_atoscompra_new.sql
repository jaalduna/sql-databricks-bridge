-- cam: j_atoscompra_new
-- columns: 32

select
    coef_01,
    coef_02,
    coef_03,
    data_compra,
    flg_scanner,
    flggranel,
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
    idsub,
    pack_comprado,
    periodo,
    preco,
    quantidade,
    unidades_packs,
    value_pm,
    FACTOR_RW1,
    Acceso_canal,
    IdApp
from j_atoscompra_new
where periodo >= {start_period} and periodo <= {end_period}
