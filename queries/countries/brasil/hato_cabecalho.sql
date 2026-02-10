-- brasil: hato_cabecalho
-- columns: 21
-- NOTE: Columns verified in SQL Server (2024-01-XX)
--       - REMOVED data_compra (per user request - use data_compra_utilizada instead)
--       - ADDED data_compra_utilizada
--       - ADDED idapp

select
    acceso_canal,
    codbarr,
    data_compra_utilizada,
    flg_scanner,
    forma_compra,
    forma_pagto,
    id_cabec,
    id_equipo,
    id_shop,
    idapp,
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
where data_compra_utilizada >= DATEADD(MONTH, -{lookback_months}, GETDATE())
