-- peru: j_atoscompra_new

select *
from j_atoscompra_new
where periodo >= {start_period} and periodo <= {end_period}
