-- cam: nac_ato
-- source: CAM_NAC.dbo.nac_ato (cross-database)

select *, CONVERT(VARCHAR(6), dataColeta, 112) AS periodo
from CAM_NAC.dbo.nac_ato
where dataColeta >= DATEADD(MONTH, -{lookback_months}, GETDATE())
