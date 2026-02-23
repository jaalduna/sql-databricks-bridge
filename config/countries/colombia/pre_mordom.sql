-- colombia: pre_mordom
-- columns: 5

select
    ano,
    iddomicilio,
    idpainel,
    mes,
    situacao
from pre_mordom
where ano >= {start_year} and ano <= {end_year}
