-- brasil: rg_panelis
-- Source: br_spri database (cross-database query)

select *
from br_spri.dbo.rg_panelis
where ano >= YEAR(DATEADD(MONTH, -{lookback_months}, GETDATE()))
