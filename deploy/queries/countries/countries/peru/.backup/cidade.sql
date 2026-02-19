-- source: peru/queries.py::get_region_kantar
-- description: retrieve distinct region data from the cidade table matching power bi's query

    select distinct
        idcidade,
        descricao
    from cidade
