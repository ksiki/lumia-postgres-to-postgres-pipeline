insert into dwh.d_product (str_id, name)
select distinct 
    prod_str_id as str_id,
    prod_name as name
from staging.transaction 
on conflict (str_id) 
do update set name = EXCLUDED.name;