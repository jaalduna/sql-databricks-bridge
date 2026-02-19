-- source: chile/queries.py :: get_region_kantar()
-- description: retrieve region data from the grupo table matching power bi's transformations

select
    idgrupo,
    descricao
from grupo
