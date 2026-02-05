-- =============================================================================
-- extraction: transaction headers (hato_cabecalho)
-- source table: bo_kwp.dbo.hato_cabecalho
-- target table: 003-precios.bronze-data.fact_jhnacps
-- parameters: 2024-01-01, 2024-12-31
-- note: elt pattern — no joins. joins performed in databricks after extraction.
-- =============================================================================

select
    h.idhato_cabecalho,
    h.iddomicilio,
    h.data_compra,
    h.idcanal,
    h.idartigo,
    h.quantidade,
    h.idpromocao,
    h.preco_unitario,
    h.tipo_ato,
    h.preco_total,
    h.forma_pagto,
    h.id_equipo,
    h.forma_compra,
    h.numpack,
    h.unipack,
    h.acceso_canal,
    h.flg_scanner,
    h.id_shop,
    h.id_cabec,
    h.codbarr

from hato_cabecalho

where h.data_compra >= convert(datetime, '2024-01-01', 120)
    and h.data_compra < convert(datetime, '2024-12-31', 120)
