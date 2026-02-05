-- bolivia: vw_venues
-- columns: 9
-- Note: Only selecting columns that exist in the actual view

select
    App,
    FORMAT_GROUP,
    FlgAtivo,
    IdCanal,
    IdTipo,
    IsRetailer,
    SECTOR,
    IdVenue,
    IdVenuePrism
from vw_venues
