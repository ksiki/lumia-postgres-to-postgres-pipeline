create schema if not exists dwh;

create table if not exists dwh.d_calendar(
    id integer not null,
    fact_date date not null,
    week_of_year smallint not null,
    month_name varchar(9) not null,
    constraint d_calendar_pk primary key (id)
);

create table if not exists dwh.d_country(
    id smallserial not null,
    name varchar(50) not null,
    constraint d_country_pk primary key (id),
    constraint d_country_name_unique unique (name)
);

create table if not exists dwh.d_city(
    id serial not null,
    name varchar(50) not null,
    country_id smallint not null,
    constraint d_city_pk primary key (id),
    constraint d_city_name_unique unique (name),
    foreign key (country_id) references dwh.d_country (id)
);

create table if not exists dwh.d_product(
    str_id varchar(100),
    name varchar(100),
    constraint d_product_pk primary key (str_id)
);

create table if not exists dwh.d_user(
    tg_id bigint not null,
    sex varchar(7) not null,
    city_id integer not null,
    registration_date_id integer not null,
    constraint d_user_pk primary key (tg_id),
    foreign key (city_id) references dwh.d_city (id),
    foreign key (registration_date_id) references dwh.d_calendar (id)
);

create table if not exists dwh.f_sales(
    date_id integer not null,
    hour smallint not null,
    prod_id varchar(100) not null,
    total_sales bigint not null,
    count_sales_with_sub bigint not null,
    count_sales_without_sub bigint not null,
    count_refunded bigint not null,
    total_revenue bigint not null,
    constraint f_sales_date_hour_unique unique (date_id, hour, prod_id),
    foreign key (date_id) references dwh.d_calendar (id),
    foreign key (prod_id) references dwh.d_product (str_id)
);
