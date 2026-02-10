-- mexico: dolar
-- columns: 5

select
    [idpais] = idpais,
    ano,
    periodo,
    tipo,
    valor
from dolar
where periodo >= CONVERT(INT, FORMAT(DATEADD(MONTH, -{lookback_months}, GETDATE()), 'yyyyMM'))
