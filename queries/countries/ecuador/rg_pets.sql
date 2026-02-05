-- ecuador: rg_pets
-- columns: 5

select
    cao_qtd,
    gato_qtd,
    iddomicilio,
    periodo,
    qtdanimais
from rg_pets
where periodo >= {start_period} and periodo <= {end_period}
