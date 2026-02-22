-- bolivia: nac_ato
-- database: BO_NAC

select *
from BO_NAC.dbo.nac_ato
where dataColeta >= DATEADD(MONTH, -{lookback_months}, GETDATE())
