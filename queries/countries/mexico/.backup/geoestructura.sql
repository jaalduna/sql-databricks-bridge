-- mexico: region lookup table
-- source: geoestructura
-- returns: region id and name mappings

select distinct
    idregion,
    region
from {schema}.geoestructura
