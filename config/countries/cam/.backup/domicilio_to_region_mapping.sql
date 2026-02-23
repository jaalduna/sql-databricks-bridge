-- source: cam/queries.py::get_domicilio_to_region_mapping()
-- description: get mapping from iddomicilio to idpais from rg_panelis table

select distinct
    p.iddomicilio,
    p.ano,
    convert(int, p.paiscam) as idpais
from rg_panelis
where p.paiscam is not null
