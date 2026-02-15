-- cam: pre_mordom
-- columns: 5
-- Note: origen_dom does not exist, using situacao instead

select
    ano,
    iddomicilio,
    idpainel,
    mes,
    situacao
from pre_mordom
where ano >= {start_year} and ano <= {end_year}
