-- brasil: rg_domicilios_pesos
-- columns: 6
-- NOTE: Column idpeso verified to exist in SQL Server (2024-01-XX)

select
    ano,
    iddomicilio,
    idpeso,
    messem,
    seqdom,
    valor
from rg_domicilios_pesos
where ano >= YEAR(DATEADD(MONTH, -{lookback_months}, GETDATE()))
