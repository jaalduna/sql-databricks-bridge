-- brasil: rg_pets
-- columns: 6
-- Source: br_spri database (cross-database query)
-- NOTE: Column ano verified to exist in SQL Server (2024-01-XX)

select
    ano,
    cao_qtd,
    gato_qtd,
    iddomicilio,
    periodo,
    qtdanimais
from br_spri.dbo.rg_pets
