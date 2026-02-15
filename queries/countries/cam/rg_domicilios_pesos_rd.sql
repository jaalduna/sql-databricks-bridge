-- cam: rg_domicilios_pesos_rd
-- columns: 5
-- Note: added alias 'periodo' for computed column

select
    ano,
    ano * 100 + messem as periodo,
    iddomicilio,
    messem,
    valor
from rg_domicilios_pesos_rd
where ano >= {start_year} and ano <= {end_year}
