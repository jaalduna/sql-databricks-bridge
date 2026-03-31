-- brasil: j_atoscompra_new_atos_scc
-- columns: 8 (idAtosSCC, idControleSCC, idAto_Origem, idAto_Destino, acao, usuario_acao, data_acao, autoriza_rejeita)

select *
from j_atoscompra_new_atos_scc
where data_acao >= DATEADD(MONTH, -{lookback_months}, GETDATE())
