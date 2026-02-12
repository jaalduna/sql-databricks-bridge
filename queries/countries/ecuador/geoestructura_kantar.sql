-- ecuador: geoestructura_kantar
-- columns: 6

select
    ksl01,
    ksl01_desc,
    ksl02,
    ksl02_desc,
    ksl01 as region_id,
    ksl01_desc as Descricao
from geoestructura_kantar
