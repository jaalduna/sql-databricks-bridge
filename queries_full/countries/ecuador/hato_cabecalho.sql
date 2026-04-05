-- ecuador: hato_cabecalho

select *, CONVERT(VARCHAR(6), data_compra, 112) AS periodo
from hato_cabecalho
where data_compra >= DATEADD(MONTH, -{lookback_months}, GETDATE())
