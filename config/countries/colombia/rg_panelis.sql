-- colombia: rg_panelis
-- columns: 10

select
    ano,
    autos,
    ciudad,
    cv,
    edac,
    iddomicilio,
    ime,
    ni,
    nse_loc,
    REGIONew
from rg_panelis
where ano >= {start_year} and ano <= {end_year}
