-- argentina: hato_cabecalho
-- columns: 17 (acceso_canal, flg_scanner, forma_compra, numpack, unipack do not exist in AR_KWP)

select
    codbarr,
    data_compra,
    data_compra_utilizada,
    forma_pagto,
    id_cabec,
    id_equipo,
    id_shop,
    idartigo,
    idcanal,
    iddomicilio,
    idhato_cabecalho,
    idmarca,
    idpromocao,
    preco_total,
    preco_unitario,
    quantidade,
    tipo_ato
from hato_cabecalho
where data_compra >= DATEADD(MONTH, -{lookback_months}, GETDATE())
