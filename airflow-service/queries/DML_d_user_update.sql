insert into dwh.d_user (tg_id, sex, city_id, registration_date_id)
select distinct
    st.tg_user_id as tg_id,
    st.sex as sex,
    cd.id as city_id,
    rd.id as registration_date_id
from staging.transaction st
join dwh.d_city cd on st.residence_city = cd.name
join dwh.d_calendar rd on st.registration_date = rd.fact_date
on conflict (tg_id) 
do update set 
    sex = EXCLUDED.sex, 
    city_id = EXCLUDED.city_id,
    registration_date_id = EXCLUDED.registration_date_id;
