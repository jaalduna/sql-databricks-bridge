-- household weights (from rg_domicilios_pesos)
-- period filter: 202501 to 202512

select
    rgp.ano,
    rgp.messem,
    (rgp.ano * 100 + rgp.messem) as periodo,
    rgp.iddomicilio,
    rgp.seqdom,
    rgp.valor
from dbo.rg_domicilios_pesos rgp
where rgp.idpeso = 1
  and (rgp.ano * 100 + rgp.messem) between 202501 and 202512
