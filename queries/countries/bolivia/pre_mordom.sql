-- bolivia: pre_mordom
-- note: using select * - schema may have changed

select *
from pre_mordom
where ano >= YEAR(DATEADD(MONTH, -{lookback_months}, GETDATE()))
