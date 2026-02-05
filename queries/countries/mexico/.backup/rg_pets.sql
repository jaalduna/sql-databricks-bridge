-- mexico: pet ownership data (mexico-specific)
-- source: rg_pets
-- returns: pet quantities per household-trimester

select
    cast(iddomicilio) + cast(ano) as iddom,
    iddomicilio,
    periodo,
    qtdanimais,
    cao_qtd,
    gato_qtd
from {schema}.rg_pets
where ano >= {start_year}
