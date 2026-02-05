-- basket headers (used for original attribute calculation)
-- downstream: attribute_calculator joins on (iddomicilio, data_compra_utilizada)
select

    idhato_cabecalho,
    iddomicilio,
    idcanal,
    data_compra,
    data_compra_utilizada,
    idartigo,
    quantidade,
    preco_unitario,
    forma_compra,
    flg_scanner,
    codbarr,
    numpack,
    unipack,
    id_shop
from dbo.hato_cabecalho (nolock)

