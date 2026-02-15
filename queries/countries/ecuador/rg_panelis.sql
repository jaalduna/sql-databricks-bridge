-- ecuador: rg_panelis
-- columns: 10 (autos, origen_dom, seqdom do not exist in EC_KWP)

select
    ano,
    ciudad,
    ciudad as region_id,
    cv,
    edac,
    edpsh,
    iddomicilio,
    ime,
    ni,
    nse_loc
from rg_panelis
where ano >= {start_year} and ano <= {end_year}
