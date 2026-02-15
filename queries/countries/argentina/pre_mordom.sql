-- argentina: pre_mordom
-- columns: 5 (origen_dom does not exist in AR_KWP, using situacao instead)

select
    ano,
    iddomicilio,
    idpainel,
    mes,
    situacao
from pre_mordom
where ano >= {start_year} and ano <= {end_year}
