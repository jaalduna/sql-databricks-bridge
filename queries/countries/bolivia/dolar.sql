-- bolivia: dolar
-- columns: 5

select
    [idpais] = idpais,
    ano,
    periodo,
    tipo,
    valor
from dolar
-- Dimension table: extract all historical data (no date filter)
