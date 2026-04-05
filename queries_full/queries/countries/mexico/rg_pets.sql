-- mexico: rg_pets

select *
from rg_pets
where periodo >= FORMAT(DATEADD(MONTH, -{lookback_months}, GETDATE()), 'yyyyMM')
