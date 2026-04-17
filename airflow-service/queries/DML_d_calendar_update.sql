with all_dates as (
    select distinct
        registration_date as date
    from staging.transaction
    union
    select distinct
        transaction_date as date
    from staging.transaction
)
insert into dwh.d_calendar (id, fact_date, week_of_year, month_name)
select 
    to_char(ad.date, 'YYYYMMDD')::integer as id,
    ad.date as fact_date,
    extract(week from ad.date)::smallint as week_of_year,
    to_char(date, 'FMMonth') as month_name
from all_dates ad
on conflict (id) do nothing;
