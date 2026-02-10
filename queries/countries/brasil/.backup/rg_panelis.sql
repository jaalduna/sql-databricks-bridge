-- panelist demographics (from rg_panelis)
-- year filter: 2025 to 2025
-- table reference: br_spri..rg_panelis (override in country config for cross-db access)

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
    ,
    rg.br_fxregiao,
    rg.ksl04,
    rg.nime18,
    rg.br_maq09,
    rg.br_maq10,
    rg.br_maq45,
    rg.br_bebe,
    rg.br_12,
    rg.bebe_mes01,
    rg.bebe_mes02,
    rg.bebe_mes03,
    rg.bebe_mes04,
    rg.bebe_mes05,
    rg.bebe_mes06,
    rg.bebe_mes07,
    rg.bebe_mes08,
    rg.bebe_mes09,
    rg.bebe_mes10,
    rg.bebe_mes11,
    rg.bebe_mes12,
    rg.imeq1,
    rg.imeq2,
    rg.imeq3,
    rg.imeq4

from br_spri..rg_panelis rg
where rg.ano between 2025 and 2025
