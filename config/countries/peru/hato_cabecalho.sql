-- peru: hato_cabecalho

select *
from hato_cabecalho
where data_compra >= {start_date} and data_compra <= {end_date}
