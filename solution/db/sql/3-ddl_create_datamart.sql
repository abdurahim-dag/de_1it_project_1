create schema if not exists staging;

create table if not exists mart.dm_change_price_in_day
(
    symbol_name             varchar(50),
    sum_vol_in_day          integer,
    price_open_in_day       numeric(10, 4),
    price_close_in_day      numeric(10, 4),
    diff_open_close_per_day numeric(4, 2),
    max_vol_in_day          timestamp with time zone,
    max_price_in_day        numeric(10, 4),
    min_price_in_day        numeric(10, 4)
);
