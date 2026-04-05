-- argentina: nac_ato

select *, CONVERT(VARCHAR(6), dataColeta, 112) AS periodo
from nac_ato
where dataColeta >= DATEADD(MONTH, -{lookback_months}, GETDATE())
