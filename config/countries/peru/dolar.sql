-- peru: dolar
-- columns: 5

select
    [idpais] = idpais,
    ano,
    periodo,
    tipo,
    valor
from dolar
where periodo >= {start_period} and periodo <= {end_period}
