-- brasil: j_atoscompra_new
-- columns: 100 (using SELECT * to capture all columns)
-- NOTE: Changed to SELECT * to include ALL 100 columns from SQL Server
--       Previous query only selected 28 columns
--       Verified columns include: factor_rw1, idapp, acceso_canal, data_compra, etc.
--       All columns will be automatically converted to lowercase in Databricks

select *
from j_atoscompra_new
where periodo >= CONVERT(INT, FORMAT(DATEADD(MONTH, -{lookback_months}, GETDATE()), 'yyyyMM'))
