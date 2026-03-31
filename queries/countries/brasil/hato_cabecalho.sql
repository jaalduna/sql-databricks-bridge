-- brasil: hato_cabecalho

select *, CONVERT(VARCHAR(6), data_compra_utilizada, 112) AS periodo
from hato_cabecalho
where data_compra_utilizada >= DATEADD(MONTH, -{lookback_months}, GETDATE())
