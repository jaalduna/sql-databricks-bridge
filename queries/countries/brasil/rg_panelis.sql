-- brasil: rg_panelis
-- columns: 37
-- Source: br_spri database (cross-database query)
-- NOTE: Column nsereg verified to exist in SQL Server (2024-01-XX)

select
    ano,
    autos,
    bebe_mes01,
    bebe_mes02,
    bebe_mes03,
    bebe_mes04,
    bebe_mes05,
    bebe_mes06,
    bebe_mes07,
    bebe_mes08,
    bebe_mes09,
    bebe_mes10,
    bebe_mes11,
    bebe_mes12,
    br_12,
    br_bebe,
    br_fxregiao,
    br_maq09,
    br_maq10,
    br_maq45,
    ciudad,
    cv,
    edac,
    edpsh,
    iddomicilio,
    ime,
    imeq1,
    imeq2,
    imeq3,
    imeq4,
    ksl04,
    ni,
    nime18,
    nse_loc,
    nsereg,
    origen_dom,
    seqdom
from br_spri.dbo.rg_panelis
where ano >= YEAR(DATEADD(MONTH, -{lookback_months}, GETDATE()))
