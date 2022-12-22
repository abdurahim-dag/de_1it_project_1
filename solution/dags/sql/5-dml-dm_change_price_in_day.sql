create or replace view mart.dm_change_price_in_day
            (
             symbol,
             day,
             sum_vol,
             price_open,
             price_close,
             change_per,
             min_vol_time,
             max_price,
             min_price
                )
as
with price as (
    select
        sy.symbol_name,
        ts.time_serial_time,
        ts.time_serial_time::date as "day",
        p.price_volume,
        p.price_close,
        p.price_open,
        p.price_high,
        p.price_low
    from
        mart.f_price p
            left join mart.d_symbol_act sy on sy.symbol_id = p.price_symbol_id
            left join mart.d_time_serial ts on ts.time_serial_id = p.price_time_id
),
     vol as (
         select distinct
             price.symbol_name,
             price.day,
             sum(price.price_volume) as "sum"
         from price
         group by price.symbol_name, price.day
     ),
     price_open as (
         select distinct
             price.symbol_name,
             price.day,
             first_value(price.price_open)
             over (partition by price.symbol_name, price.day order by price.time_serial_time) as "value"
         from price
     ),
     price_close as (
         select distinct
             price.symbol_name,
             price.day,
             first_value(price.price_open)
             over (partition by price.symbol_name, price.day order by price.time_serial_time desc) as "value"
         from price
     ),
     max_vol as (
         select distinct
             price.symbol_name,
             price.day,
             first_value(price.time_serial_time)
             over (partition by price.symbol_name, price.day order by price.price_volume desc) as "value"
         from price
     ),
     max_price as (
         select distinct
             price.symbol_name,
             price.day,
             max(price.price_high) as "value"
         from price
         group by price.symbol_name,
                  price.day
     ),
     min_price as (
         select distinct
             price.symbol_name,
             price.day,
             min(price.price_high) as "value"
         from price
         group by price.symbol_name,
                  price.day
     )

select
    p.symbol_name,
    p.day,
    vol.sum,
    price_open.value,
    price_close.value,
    ((price_close.value - price_open.value) / price_open.value * 100),
    max_vol.value,
    max_price.value,
    min_price.value
from (select distinct
          price.symbol_name,
          price.day
      from price) p
         left join vol on vol.day=p.day and vol.symbol_name=p.symbol_name
         left join price_open on price_open.day=p.day and price_open.symbol_name=p.symbol_name
         left join price_close on price_close.day=p.day and price_close.symbol_name=p.symbol_name
         left join max_vol on max_vol.day=p.day and max_vol.symbol_name=p.symbol_name
         left join max_price on max_price.day=p.day and max_price.symbol_name=p.symbol_name
         left join min_price on min_price.day=p.day and min_price.symbol_name=p.symbol_name
order by p.day desc