-- source: peru/queries.py::get_rg_pets
-- description: queries rg_pets table and returns processed dataframe matching power bi transformations
-- filter: ano >= 2021

    select
        cast(iddomicilio) + cast(ano) as iddom,
        iddomicilio,
        periodo,
        qtdanimais,
        cao_qtd,
        gato_qtd
    from rg_pets
    where ano >= 2021
