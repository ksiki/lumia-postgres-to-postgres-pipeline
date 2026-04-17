delete from dwh.f_sales 
where date_id = (select id from dwh.d_calendar where fact_date = '{{ ds }}'::date) 
    and hour = {{ logical_date.strftime('%H') | int }};

insert into dwh.f_sales (
    date_id, 
    hour, 
    prod_id, 
    total_sales, 
    count_sales_with_sub, 
    count_sales_without_sub, 
    count_refunded, 
    total_revenue
)
select
    dc.id as date_id,
    extract(hour from st.transaction_time) as hour,
    dp.str_id as prod_id,
    count(*) as total_sales,
    count(*) filter (where st.is_subscription_active) as count_sales_with_sub,
    count(*) filter (where not st.is_subscription_active) as count_sales_without_sub,
    count(*) filter (where st.refunded) as count_refunded,
    sum(st.stars_price_actual) as total_revenue
from staging.transaction st
join dwh.d_calendar dc on st.transaction_date = dc.fact_date
join dwh.d_product dp on st.prod_str_id = dp.str_id
where dc.fact_date = '{{ ds }}'::date
    and extract(hour from st.transaction_time) = {{ logical_date.strftime('%H') | int }}
group by dc.id, extract(hour from st.transaction_time), dp.str_id
on conflict (date_id, hour, prod_id) do nothing;