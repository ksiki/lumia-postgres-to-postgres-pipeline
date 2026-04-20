with users_actual_data as (
    select 
        du.user_id,
        du.sex,
        rs.name as city_name,
        dc.date as reg_date
    from dwh.d_user du
    left join dwh.d_city rs on du.residence_city_id = rs.id
    left join dwh.d_calendar dc on du.registration_date_id = dc.id
    where du.is_current = True
)
select 
    du.user_id as user_id,
    uad.sex as sex,
    uad.city_name as residence_city,
    uad.reg_date as registration_date,
    dp.str_id as prod_str_id,
    dp.name as prod_name,
    dp.category as prod_category,
    dc.date as transaction_date,
    ft.time as transaction_time,
    ft.stars_price_original as stars_price_original,
    ft.stars_price_actual as stars_price_actual,
    ft.is_subscription_active as is_subscription_active,
    (case ft.status when 'refund' then 1 else 0 end)::bool as refunded
from dwh.f_transaction ft
left join dwh.d_user du on ft.user_id = du.id
left join users_actual_data uad on du.user_id = uad.user_id
left join dwh.d_product dp on ft.product_id = dp.id
left join dwh.d_calendar dc on ft.date_id = dc.id
where 
    dc.date = '{date_param}'
    and extract(hour from ft.time) = '{hour_param}';