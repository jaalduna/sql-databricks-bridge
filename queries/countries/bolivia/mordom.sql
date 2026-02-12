-- bolivia: mordom

select *
from mordom
where ano >= YEAR(DATEADD(MONTH, -{lookback_months}, GETDATE()))
