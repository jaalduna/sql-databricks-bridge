-- bolivia: domicilio_posse_bens
-- columns: 3

select
    iddomicilio,
    idposse_bem,
    quantidade
from domicilio_posse_bens
where YEAR(data) >= YEAR(DATEADD(MONTH, -{lookback_months}, GETDATE()))
