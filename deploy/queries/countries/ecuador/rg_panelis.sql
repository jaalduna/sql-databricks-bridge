-- ecuador: rg_panelis
-- columns: 12 (autos, origen_dom, seqdom do not exist in EC_KWP)
-- ksl01/ksl02 are required for region mapping via geoestructura_kantar

select
    ano,
    ciudad,
    cast(ksl01 as int) as ksl01,
    cast(ksl02 as int) as ksl02,
    cv,
    edac,
    edpsh,
    iddomicilio,
    ime,
    ni,
    nse_loc
from rg_panelis
where ano >= {start_year} and ano <= {end_year}
