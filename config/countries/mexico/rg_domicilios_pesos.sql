-- mexico: rg_domicilios_pesos

select *, ano*100+messem as periodo
from rg_domicilios_pesos
where ano >= {start_year} and ano <= {end_year}
