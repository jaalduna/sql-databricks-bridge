-- argentina: rg_panelis
-- columns: 6 (ciudad, edpsh, ime, ni, nse_loc, origen_dom, seqdom do not exist in AR_KWP)

select
    ano,
    autos,
    cv,
    edac,
    iddomicilio,
    nse_saimo
from rg_panelis
where ano >= {start_year} and ano <= {end_year}
