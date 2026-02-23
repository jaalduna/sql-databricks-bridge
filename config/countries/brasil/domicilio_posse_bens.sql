-- brasil: domicilio_posse_bens
-- columns: 3

select
    iddomicilio,
    idposse_bem,
    quantidade,
    YEAR(data)*100+MONTH(data) as periodo
from domicilio_posse_bens
where YEAR(data) >= YEAR(DATEADD(MONTH, -{lookback_months}, GETDATE()))
