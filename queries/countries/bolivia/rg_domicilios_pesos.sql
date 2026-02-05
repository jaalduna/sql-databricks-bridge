-- bolivia: rg_domicilios_pesos
-- columns: 5

select
    ano,
    iddomicilio,
    messem,
    seqdom,
    valor
from rg_domicilios_pesos
where ano >= YEAR(DATEADD(MONTH, -{lookback_months}, GETDATE()))
