-- argentina: hato_cabecalho
-- Use start_period/end_period (derived from lookback_months) to match
-- the same date range as j_atoscompra_new.

select *, CONVERT(VARCHAR(6), data_compra, 112) AS periodo
from hato_cabecalho
where data_compra >= {start_date}
