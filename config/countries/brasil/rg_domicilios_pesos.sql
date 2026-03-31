-- brasil: rg_domicilios_pesos
-- columns: 5 (seqdom removed - no longer exists)
-- NOTE: Column idpeso verified to exist in SQL Server (2024-01-XX)

select
    ano,
    iddomicilio,
    idpeso,
    messem,
    valor,
    ano*100+messem as periodo
from rg_domicilios_pesos
where ano >= {start_year} and ano <= {end_year}
