create schema if not exists staging;

create table if not exists staging.transaction (
    tg_user_id bigint,
    sex varchar(7),
    residence_city varchar(50),
    residence_country varchar(50),
    registration_date date,
    prod_str_id varchar(100),
    prod_name varchar(100),
    transaction_date date,
    transaction_time time,
    stars_price_original smallint,
    stars_price_actual smallint,
    is_subscription_active bool,
    refunded bool
); 
