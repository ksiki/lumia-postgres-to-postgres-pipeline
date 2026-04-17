insert into dwh.d_country (name)
select distinct
    residence_country as name
from staging.transaction
on conflict (name) do nothing;