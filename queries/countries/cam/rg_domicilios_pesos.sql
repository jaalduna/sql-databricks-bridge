-- cam: rg_domicilios_pesos
-- columns: 5
-- Note: seqdom does not exist in CAM

select
    ano,
    iddomicilio,
    idpeso,
    messem,
    valor
from rg_domicilios_pesos
where ano >= {start_year} and ano <= {end_year}
