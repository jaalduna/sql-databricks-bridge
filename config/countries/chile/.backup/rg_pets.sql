-- source: chile/queries.py :: get_rg_pets()
-- description: queries rg_pets table and returns a dataframe matching power bi's processing steps

select
    cast(iddomicilio) + cast(ano) as iddom,
    iddomicilio,
    periodo,
    qtdanimais,
    cao_qtd,
    gato_qtd
from rg_pets
where ano >= 2022
