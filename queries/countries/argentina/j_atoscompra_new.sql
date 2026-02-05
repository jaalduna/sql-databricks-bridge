-- argentina: j_atoscompra_new
-- columns: 11

select
    data_compra,
    flg_scanner,
    flggranel,
    idartigo,
    idato,
    iddomicilio,
    idfabricante,
    idmarca,
    idpainel,
    idproduto,
    periodo
from j_atoscompra_new
where periodo >= {start_period} and periodo <= {end_period}
