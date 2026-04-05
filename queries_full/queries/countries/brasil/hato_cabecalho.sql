-- brasil: hato_cabecalho

select *
from hato_cabecalho
where data_compra_utilizada >= DATEADD(MONTH, -{lookback_months}, GETDATE())
