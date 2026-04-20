insert into dwh.f_user_analytics (
    user_id,
    first_purchases_date_id, 
    last_purchases_date_id, 
    total_purchases,
    service_purchases,
    sub_purchases,
    mtx_purchases,
    total_revenue,
    sub_revenue,
    mtx_revenue
)
select
    du.tg_id as user_id,
    min(dc.id) as first_purchases_date_id,
    max(dc.id) as last_purchases_date_id,
    count(*) as total_purchases,
    count(*) filter (where st.prod_category like '%_service') as service_purchases,
    count(*) filter (where st.prod_category = 'subscription') as sub_purchases,
    count(*) filter (where st.prod_category = 'microtransaction') as mtx_purchases,
    sum(st.stars_price_actual) as total_revenue,
    coalesce(
        sum(st.stars_price_actual) filter (where st.prod_category = 'subscription'),
        0
    ) as sub_revenue,
    coalesce(
        sum(st.stars_price_actual) filter (where st.prod_category = 'microtransaction'),
        0
    ) as mtx_revenue
from staging.transaction st
join dwh.d_user du on st.tg_user_id = du.tg_id
join dwh.d_calendar dc on st.transaction_date = dc.fact_date
group by du.tg_id
on conflict (user_id) do update set
    last_purchases_date_id = excluded.last_purchases_date_id,
    total_purchases = excluded.total_purchases,
    service_purchases = excluded.service_purchases,
    sub_purchases = excluded.sub_purchases,
    mtx_purchases = excluded.mtx_purchases,
    total_revenue = excluded.total_revenue,
    sub_revenue = excluded.sub_revenue,
    mtx_revenue = excluded.mtx_revenue;