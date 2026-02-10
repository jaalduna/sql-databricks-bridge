-- table: rg_pets
-- description: pet ownership data
-- source: brasil/queries.py::get_rg_pets()

select
    cast(iddomicilio) + cast(ano) as iddom,
    iddomicilio,
    periodo,
    qtdanimais,
    cao_qtd,
    gato_qtd
from rg_pets
where ano >= 2022
