-- mexico: nac_ato
-- source: mx_sinc.dbo.nac_ato (cross-database)

select *
from mx_sinc.dbo.nac_ato
where duplicado = 0
    and datacoleta >= DATEADD(MONTH, -{lookback_months}, GETDATE())
