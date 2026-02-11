-- purchase transactions (from j_atoscompra_new)
-- period filter: 202501 to 202512

select
    j.idato,
    j.iddomicilio,
    j.idproduto,
    j.idartigo,
    j.idfabricante,
    j.idmarca,
    j.data_compra,
    j.periodo,
    j.flggranel,
    j.idpainel,
    j.flg_scanner,
    datepart(ww, j.data_compra) as semana,
    concat(
        cast(j.iddomicilio as varchar), '/',
        convert(varchar, j.data_compra, 23), '/',
        cast(j.idcanal as varchar), '/',
        cast(j.acceso_canal as varchar), '/',
        cast(j.idapp as varchar)
    ) as idocas
from dbo.j_atoscompra_new
where j.periodo between 202501 and 202512
  and j.flg_scanner <= 5
