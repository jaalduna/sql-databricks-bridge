-- =============================================================================
-- extraction: sales channel dimension (a_canal)
-- source table: mx_kwp.dbo.a_canal
-- target table: 003-precios.bronze-data.dim_canal
-- parameters: none (full dimension extract)
-- =============================================================================

select
    ac.idcanal,
    ac.idtipo,
    ac.descricao,
    ac.dtcriacao,
    ac.dtatualizacao,
    ac.flgativo,
    ac.sempreco,
    ac.presencial,
    ac.internet,
    ac.telefono,
    ac.app,
    ac.whatsapp,
    ac.rsocial,
    ac.isretailer,
    ac.regalo

from a_canal ac
