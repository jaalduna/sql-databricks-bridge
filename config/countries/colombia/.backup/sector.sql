-- product sector definitions
-- downstream: used for sector-level calibration grouping
select

    idsector,
    descricao,
    flgativo
from dbo.sector (nolock)

