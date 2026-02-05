-- household master data (from domicilios)

select
    d.iddomicilio,
    d.origen,
    d.idcidade
from dbo.domicilios
