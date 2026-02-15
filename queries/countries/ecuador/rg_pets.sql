-- ecuador: rg_pets
-- columns: 5 (periodo is varchar 'Q1_2017' format, filter by ano instead)

select
    cao_qtd,
    gato_qtd,
    iddomicilio,
    periodo,
    qtdanimais
from rg_pets
where ano >= {start_year} and ano <= {end_year}
