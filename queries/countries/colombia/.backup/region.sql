-- geographic region mapping
-- downstream: used to map region_id to region name
-- note: some countries (bolivia) use hardcoded mapping in config instead
select distinct

    idgrupo,
    descricao
    
    
from dbo.mbdcidade_2 (nolock)

