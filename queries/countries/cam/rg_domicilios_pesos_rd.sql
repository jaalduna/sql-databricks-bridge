-- cam: rg_domicilios_pesos_rd
-- columns: 5

select
    ano,
    ano * 100 + messem,
    iddomicilio,
    messem,
    valor
from rg_domicilios_pesos_rd
where ano >= {start_year} and ano <= {end_year}
