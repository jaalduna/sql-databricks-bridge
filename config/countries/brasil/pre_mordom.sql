-- brasil: pre_mordom
-- columns: 6

select
    ano,
    iddomicilio,
    idpainel,
    mes,
    origen_dom,
    situacao
from pre_mordom
where ano >= YEAR(DATEADD(MONTH, -{lookback_months}, GETDATE()))
