-- bolivia: region lookup table
-- source: cidade
-- returns: region id and name mappings

select distinct
    idcidade,
    descricao
from {schema}.cidade
