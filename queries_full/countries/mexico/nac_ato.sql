-- mexico: nac_ato
-- source: mx_sinc.dbo.nac_ato (cross-database)

select *, CONVERT(VARCHAR(6), datacoleta, 112) AS periodo
from mx_sinc.dbo.nac_ato
where duplicado = 0
    and datacoleta >= DATEADD(MONTH, -{lookback_months}, GETDATE())
