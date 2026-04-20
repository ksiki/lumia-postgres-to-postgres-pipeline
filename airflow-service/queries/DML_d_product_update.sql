insert into dwh.d_product (str_id, name, category)
select distinct 
    prod_str_id as str_id,
    prod_name as name,
    prod_category as category
from staging.transaction 
on conflict (str_id) 
do update set name = EXCLUDED.name;