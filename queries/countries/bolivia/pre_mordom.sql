-- bolivia: pre_mordom
-- columns: 5

select
    ano,
    iddomicilio,
    idpainel,
    mes,
    origen_dom
from pre_mordom
where ano >= YEAR(DATEADD(MONTH, -{lookback_months}, GETDATE()))
