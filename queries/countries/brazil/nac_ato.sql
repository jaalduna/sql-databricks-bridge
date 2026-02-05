-- brazil: nac_ato
-- columns: 14
-- Source: BR_NAC database (cross-database query)

select
    coef01,
    coef02,
    coef03,
    datacoleta,
    duplicado,
    frmcompra,
    idato,
    numpack,
    preco,
    qtde,
    unipack,
    ventasuelta,
    volume,
    vsfrmcompra
from BR_NAC.dbo.nac_ato
