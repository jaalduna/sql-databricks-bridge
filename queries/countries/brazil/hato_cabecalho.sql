-- brazil: hato_cabecalho
-- columns: 20

select
    acceso_canal,
    codbarr,
    data_compra,
    flg_scanner,
    forma_compra,
    forma_pagto,
    id_cabec,
    id_equipo,
    id_shop,
    idartigo,
    idcanal,
    iddomicilio,
    idhato_cabecalho,
    idpromocao,
    numpack,
    preco_total,
    preco_unitario,
    quantidade,
    tipo_ato,
    unipack
from hato_cabecalho
where data_compra >= {start_date} and data_compra <= {end_date}
