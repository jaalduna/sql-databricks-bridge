-- cam: nac_ato
-- source: CAM_NAC.dbo.nac_ato (cross-database)

select *
from CAM_NAC.dbo.nac_ato
where dataColeta >= DATEADD(MONTH, -{lookback_months}, GETDATE())
