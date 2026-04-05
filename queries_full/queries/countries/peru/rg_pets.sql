-- peru: rg_pets
-- columns: 5 (periodo is varchar 'Q2_2019' format, filter by ano instead)

select
    cao_qtd,
    gato_qtd,
    iddomicilio,
    periodo,
    qtdanimais
from rg_pets
where ano >= {start_year} and ano <= {end_year}
