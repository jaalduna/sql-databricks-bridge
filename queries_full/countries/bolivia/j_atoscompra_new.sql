-- bolivia: j_atoscompra_new

select *
from j_atoscompra_new
where periodo >= FORMAT(DATEADD(MONTH, -{lookback_months}, GETDATE()), 'yyyyMM')
