-- colombia: rg_pets
-- columns: 5

select
    cao_qtd,
    gato_qtd,
    iddomicilio,
    periodo,
    qtdanimais
from rg_pets
where ano >= {start_year} and ano <= {end_year}
