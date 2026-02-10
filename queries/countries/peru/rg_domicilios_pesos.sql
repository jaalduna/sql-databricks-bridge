-- peru: rg_domicilios_pesos
-- columns: 6

select
    ano,
    iddomicilio,
    idpeso,
    messem,
    seqdom,
    valor
from rg_domicilios_pesos
where ano >= {start_year} and ano <= {end_year}
