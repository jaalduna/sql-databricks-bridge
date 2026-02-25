-- mexico: pre_mordom

select *
from pre_mordom
where ano >= YEAR(DATEADD(MONTH, -{lookback_months}, GETDATE()))
