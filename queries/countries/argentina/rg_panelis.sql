-- argentina: rg_panelis
-- columns: 12

select
    ano,
    autos,
    ciudad,
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
