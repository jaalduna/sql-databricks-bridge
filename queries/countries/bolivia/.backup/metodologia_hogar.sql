-- bolivia: household methodology metrics
-- source: derived from j_atoscompra_new + hato_cabecalho
-- returns: aggregated purchase behavior metrics and methodology type per household-period

with pnc as (
    select
        j.iddomicilio,
        j.periodo,
        count(distinct h.flg_scanner) as cant_met,
        count(distinct datepart(ww, j.data_compra)) as semanas,
        count(j.idato) as actos_x_hogar,
        count(distinct j.idproduto) as categorias_x_hogar,
        count(distinct concat(
            convert(date, j.data_compra),
            j.idcanal,
            j.idapp,
            j.acceso_canal
        )) as viajes_x_hogar,
        count(case when h.flg_scanner = 1 then 0 end) as actos_ibs,
        count(case when h.flg_scanner = 2 then 0 end) as actos_ihs,
        max(h.flg_scanner) as metodologia
    from {schema}.j_atoscompra_new j (nolock)
    left join {schema}.hato_cabecalho h (nolock)
        on j.idhato_cabecalho = h.idhato_cabecalho
    where j.iddomicilio = h.iddomicilio
        and convert(date, j.data_compra) = convert(date, h.data_compra_utilizada)
        and j.periodo >= {start_period}
        and j.idpainel not in (14, 99)
        and h.flg_scanner between 1 and 2
        and j.idproduto <> 999
    group by j.iddomicilio, j.periodo
),
tivian as (
    select
        j.iddomicilio,
        j.periodo,
        count(j.idato) as actos_tivian
    from {schema}.j_atoscompra_new j (nolock)
    left join {schema}.hato_cabecalho h (nolock)
        on j.idhato_cabecalho = h.idhato_cabecalho
    where (
            (
                j.iddomicilio = h.iddomicilio
                and convert(date, j.data_compra) = convert(date, h.data_compra_utilizada)
            )
            or
            (
                j.periodo in (202408,202409)
                and j.flg_scanner in (1, 2)
            )
        )
        and j.periodo >= {start_period}
        and j.idpainel not in (14, 99)
        and h.flg_scanner = 3
        and j.idproduto <> 999
    group by j.iddomicilio, j.periodo
)
select
    coalesce(p.iddomicilio, t.iddomicilio) as iddomicilio,
    coalesce(p.periodo, t.periodo) as periodo,
    cast(coalesce(p.iddomicilio, t.iddomicilio) as varchar)
        + cast(coalesce(p.periodo, t.periodo) as varchar) as idpeso,
    p.cant_met,
    p.semanas,
    p.actos_x_hogar,
    p.categorias_x_hogar,
    p.viajes_x_hogar,
    p.actos_ibs,
    p.actos_ihs,
    t.actos_tivian,
    p.metodologia
from pnc
full join tivian
    on p.iddomicilio = t.iddomicilio
    and p.periodo = t.periodo
