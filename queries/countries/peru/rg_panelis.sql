-- peru: rg_panelis
-- columns: 9 (edpsh, origen_dom, seqdom do not exist in PE_KWP)

select
    ano,
    autos,
    ciudad,
    cv,
    edac,
    iddomicilio,
    ime,
    ni,
    nse_loc
from rg_panelis
where ano >= {start_year} and ano <= {end_year}
