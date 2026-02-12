-- ecuador: rg_panelis
-- columns: 13

select
    ano,
    autos,
    ciudad,
    ciudad as region_id,
    cv,
    edac,
    edpsh,
    iddomicilio,
    ime,
    ni,
    nse_loc,
    origen_dom,
    seqdom
from rg_panelis
where ano >= {start_year} and ano <= {end_year}
