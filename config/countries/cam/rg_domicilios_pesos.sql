-- cam: rg_domicilios_pesos
-- columns: 5
-- Note: seqdom does not exist in CAM

select
    ano,
    iddomicilio,
    idpeso,
    messem,
    valor,
    ano*100+messem as periodo
from rg_domicilios_pesos
where ano >= {start_year} and ano <= {end_year}
