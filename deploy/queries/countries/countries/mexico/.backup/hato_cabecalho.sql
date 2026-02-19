-- mexico: basket headers
-- source: hato_cabecalho
-- returns: basket-level metadata including scanner flag and barcode info
-- used to determine original vs scc classification

select
    h.idhato_cabecalho,
    h.iddomicilio,
    h.data_compra_utilizada,
    h.flg_scanner,
    h.codbarr
from {schema}.hato_cabecalho h with (nolock)
where h.flg_scanner is not null
