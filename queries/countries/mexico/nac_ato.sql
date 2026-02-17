-- mexico: nac_ato
-- source: mx_sinc.dbo.nac_ato (cross-database)
-- columns: 14

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
from mx_sinc.dbo.nac_ato
where duplicado = 0
    and datacoleta >= DATEADD(MONTH, -{lookback_months}, GETDATE())
