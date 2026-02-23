-- mexico: cm_region_estrato
-- columns: 3

select distinct
    ISNULL(id_cidade, 0) as ciudad,
    ISNULL(id_region, 0) as idregion,
    case
        when id_region = 1 then 'Noroeste'
        when id_region = 9 then 'Tijuana'
        when id_region = 7 then 'Monterrey'
        when id_region = 2 then 'Noreste'
        when id_region = 8 then 'Guadalajara'
        when id_region = 10 then 'Leon'
        when id_region = 3 then 'Occidente'
        when id_region = 11 then 'Puebla'
        when id_region = 4 then 'Centro'
        when id_region = 5 then 'AMCM'
        when id_region = 12 then 'Merida'
        when id_region = 6 then 'Sureste'
    end as region
from cm_region_estrato
