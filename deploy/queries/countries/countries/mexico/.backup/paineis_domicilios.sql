-- panel-household relationships (from paineis_domicilios)

select
    pd.idpainel,
    pd.iddomicilio,
    pd.data_entrada,
    pd.data_saida
from dbo.paineis_domicilios pd
