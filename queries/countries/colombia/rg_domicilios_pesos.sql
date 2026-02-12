-- colombia: rg_domicilios_pesos
-- columns: 5

select
    ano,
    iddomicilio,
    idpeso,
    messem,
    valor
from rg_domicilios_pesos
where ano >= {start_year} and ano <= {end_year}
