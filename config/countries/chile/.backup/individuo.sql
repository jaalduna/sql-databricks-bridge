-- household members
-- downstream: used for household size and demographic enrichment
select

    idindividuo,
    iddomicilio,
    sexo,
    idade,
    parentesco,
    flgativo
from dbo.individuo (nolock)

