-- source: peru/queries.py::get_mf
-- description: retrieve the mf table exactly matching power bi's structure and transformations
-- note: this is the base query; columns cdc06, clas06, cdc07, clas07, cdc08, clas08, cdc09, clas09, minprice, maxprice, nroactosmat, nrobuyers, nrobuyersmat are removed in post-processing

select * from vw_artigoz
