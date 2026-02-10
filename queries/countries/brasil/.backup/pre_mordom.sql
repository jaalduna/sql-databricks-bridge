-- household mortality status (from pre_mordom)
-- period filter: 202501 to 202512

select
    pmd.idpainel,
    pmd.ano,
    pmd.mes,
    (pmd.ano * 100 + pmd.mes) as periodo,
    pmd.iddomicilio,
    pmd.origen_dom
from dbo.pre_mordom pmd
where pmd.situacao = 1
  and (pmd.ano * 100 + pmd.mes) between 202501 and 202512
