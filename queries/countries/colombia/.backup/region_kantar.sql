-- source: colombia/queries.py - get_region_kantar()
-- description: retrieve distinct region data from the mbdcidade_2 table matching power bi's transformations
-- tables: mbdcidade_2

select distinct
    convert(int, idgrupo) as idregion,
    descricao2
from mbdcidade_2
