-- source: cam/queries.py::get_rg_domicilios_pesos_rd()
-- description: retrieve and process rg_domicilios_pesos_rd data matching power bi transformations

select
    dp.ano,
    dp.messem,
    dp.iddomicilio,
    dp.valor,
    dp.ano * 100 + dp.messem,
    cast(dp.iddomicilio) + cast(dp.ano * 100 + dp.messem) as idpeso,
    cast(dp.iddomicilio) + cast(dp.ano) as iddom,
    convert(int,p.paiscam) as idpais,
    cast(convert(int,p.paiscam) as varchar) + cast(convert(int,p.ksl01) as varchar) as idregion,
    convert(int,p.mirror01) as mirror01,
    convert(int,p.mirror02) as mirror02,
    convert(int,p.mirror03) as mirror03,
    convert(int,p.mirror04) as mirror04,
    convert(int,p.mirror05) as mirror05,
    convert(int,p.mirror06) as mirror06,
    convert(int,p.mirror07) as mirror07,
    convert(int,p.mirror08) as mirror08,
    convert(int,p.mirror09) as mirror09,
    convert(int,p.mirror10) as mirror10,
    convert(int,p.mirror11) as mirror11,
    convert(int,p.mirror12) as mirror12,
    convert(int,p.caricam01) as caricam01,
    convert(int,p.caricam02) as caricam02,
    convert(int,p.caricam03) as caricam03,
    convert(int,p.caricam04) as caricam04,
    convert(int,p.caricam05) as caricam05,
    convert(int,p.caricam06) as caricam06,
    convert(int,p.caricam07) as caricam07,
    convert(int,p.caricam08) as caricam08,
    convert(int,p.caricam09) as caricam09,
    convert(int,p.caricam10) as caricam10,
    convert(int,p.caricam11) as caricam11,
    convert(int,p.caricam12) as caricam12

from rg_domicilios_pesos_rd dp
left join rg_panelis on dp.iddomicilio = p.iddomicilio and dp.ano = p.ano
where dp.idpeso = 1
and convert(int,dp.ano*100+dp.messem) >= 202201
