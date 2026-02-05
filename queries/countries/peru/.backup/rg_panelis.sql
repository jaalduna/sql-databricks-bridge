-- panelist demographics (from rg_panelis)
-- year filter: 2025 to 2025
-- table reference: {schema}.rg_panelis (override in country config for cross-db access)

select
    rg.iddomicilio,
    rg.ano,
    rg.seqdom,
    rg.origen_dom,
    rg.ciudad,
    rg.nse_loc,
    rg.edac,
    rg.ime,
    rg.edpsh,
    rg.ni,
    rg.cv,
    rg.autos
    
from {schema}.rg_panelis rg
where rg.ano between 2025 and 2025
