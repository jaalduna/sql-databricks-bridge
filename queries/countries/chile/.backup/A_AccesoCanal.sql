-- =============================================================================
-- extraction: channel access dimension (a_accesocanal)
-- source table: cl_kwp.dbo.a_accesocanal
-- target table: 003-precios.bronze-data.dim_acceso_canal
-- parameters: none (full dimension extract)
-- =============================================================================

select
    aac.idacceso,
    aac.descricao,
    aac.datacriacao,
    aac.dataatualizacao,
    aac.flgativo

from a_accesocanal aac
