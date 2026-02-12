-- brasil: hato_ruim_cabecalho

select *
from hato_ruim_cabecalho
where data_criacao >= DATEADD(DAY, -90, GETDATE())
