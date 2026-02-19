-- argentina: hato_cabecalho

select *
from hato_cabecalho
where data_compra >= DATEADD(MONTH, -{lookback_months}, GETDATE())
