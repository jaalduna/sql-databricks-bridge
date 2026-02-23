-- ecuador: nac_ato

select *
from nac_ato
where dataColeta >= DATEADD(MONTH, -{lookback_months}, GETDATE())
