-- bolivia: rg_panelis
-- note: using select * - schema may have changed

select *
from rg_panelis
where ano >= YEAR(DATEADD(MONTH, -{lookback_months}, GETDATE()))
