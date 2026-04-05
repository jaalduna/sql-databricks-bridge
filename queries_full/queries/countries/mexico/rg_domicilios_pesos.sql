-- mexico: rg_domicilios_pesos

select *
from rg_domicilios_pesos
where ano >= YEAR(DATEADD(MONTH, -{lookback_months}, GETDATE()))
