-- bolivia: nac_ato
-- database: BO_NAC

select *, CONVERT(VARCHAR(6), dataColeta, 112) AS periodo
from BO_NAC.dbo.nac_ato
where dataColeta >= DATEADD(MONTH, -{lookback_months}, GETDATE())
