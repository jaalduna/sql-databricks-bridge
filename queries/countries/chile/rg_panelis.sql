-- chile: rg_panelis
-- columns: 33 (autos, edpsh, nse_loc, origen_dom, seqdom do not exist in CL_KWP)

select
    iddomicilio,
    ano,
    pais,
    npan,
    edac,
    ni,
    ime,
    zona,
    ciudad,
    edma,
    cv,
    p_c12,
    cl_obs,
    lab,
    imc,
    grupo,
    edbeq1,
    edbeq2,
    edbeq3,
    edbeq4,
    nsereg,
    planta,
    embotelladora,
    nime18,
    edjf,
    cmedia,
    new_sample,
    nse_aim,
    region_orion,
    hogar_ko,
    mirror,
    extranjero,
    nro_hab
from rg_panelis
where ano >= {start_year} and ano <= {end_year}
