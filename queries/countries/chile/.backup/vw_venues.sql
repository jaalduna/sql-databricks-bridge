-- channel/venue classification
-- downstream: used for channel_level_1 (sector) and channel_level_2 (format_group) mapping
select

    idcanal,
    idtipo,
    descricao,
    sector,
    format_group,
    sempreco,
    presencial,
    internet,
    telefono,
    app,
    whatsapp,
    rsocial,
    isretailer,
    regalo,
    flgativo
from dbo.vw_venues (nolock)

