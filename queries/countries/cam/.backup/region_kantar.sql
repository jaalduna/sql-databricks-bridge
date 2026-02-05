-- source: cam/queries.py::get_region_kantar()
-- description: retrieve region data from geoestructura_detalle table matching power bi's transformations

select distinct
    idpais,
    pais,
    idregion,
    region
from geoestructura_detalle
