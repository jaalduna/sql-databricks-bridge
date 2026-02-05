-- bolivia: rg_domicilios_pesos
-- note: using select * - schema may have changed

select *
from rg_domicilios_pesos
where ano >= YEAR(DATEADD(MONTH, -{lookback_months}, GETDATE()))
