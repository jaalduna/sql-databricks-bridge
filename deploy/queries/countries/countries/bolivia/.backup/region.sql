-- geographic region mapping
-- downstream: used to map region_id to region name
-- note: some countries (bolivia) use hardcoded mapping in config instead
select distinct

    idcidade,
    descricao
    
    
from dbo.cidade (nolock)

