-- chile: mordom

select *, ano as periodo
from mordom
where ano >= {start_year} and ano <= {end_year}
