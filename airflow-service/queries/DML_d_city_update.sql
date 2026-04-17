insert into dwh.d_city (name, country_id)
select distinct
    residence_city as name,
    dc.id as country_id
from staging.transaction st
join dwh.d_country dc on st.residence_country = dc.name
on conflict (name) do nothing;