-- mexico: mordom

select *, ano as periodo
from mordom
where ano >= YEAR(DATEADD(MONTH, -{lookback_months}, GETDATE()))
