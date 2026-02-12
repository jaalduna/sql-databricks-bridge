-- chile: rg_domicilios_pesos
-- columns: 5 (seqdom does not exist in CL_KWP)

select
    ano,
    iddomicilio,
    idpeso,
    messem,
    valor
from rg_domicilios_pesos
where ano >= {start_year} and ano <= {end_year}
