-- source: ecuador/queries.py - get_region_kantar()
-- description: retrieve distinct region data from the geoestructura_kantar table

    select distinct
        ksl01,
        ksl01_desc,
        ksl02,
        ksl02_desc,
        cast(ksl01) + cast(ksl02) as idregion
    from geoestructura_kantar
