-- brasil: cidade (region self-mapping)
-- Brasil uses BR_FXREGIAO values directly as region names in rg_panelis.
-- This query creates a lookup table mapping those values to themselves.
-- Source: br_spri database

select distinct
    br_fxregiao as region_id,
    br_fxregiao as region,
    br_fxregiao as Descricao
from br_spri.dbo.rg_panelis
where br_fxregiao is not null
