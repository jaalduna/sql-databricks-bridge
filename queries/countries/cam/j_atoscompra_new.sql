-- cam: j_atoscompra_new
-- columns: 12

select
    data_compra,
    factor_rw1,
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
