-- mexico: rg_panelis

select *
from rg_panelis
where ano >= YEAR(DATEADD(MONTH, -{lookback_months}, GETDATE()))
