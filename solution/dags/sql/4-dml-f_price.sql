with upload as (
    select
        u.upload_id,
        u.symbol_name,
        u.interval_name
    from staging.upload_hist u
    where u.upload_id not in (select upload_id from staging.upload_hist where symbol_name=u.symbol_name and date='{{ds}}' and uploaded is true)
    and u.uploaded is not true
),
     upload_stocks_interval as (
         select
             s.time,
             s.open,
             s.close,
             s.high,
             s.low,
             s.volume,
             s.upload_id,
             interval.interval_id,
             sa.symbol_id
         from staging.stocks s
                  join upload u on u.upload_id = s.upload_id
                  join mart.d_interval interval
                       on interval.interval_name = u.interval_name
                  join mart.d_symbol_act sa
                       on sa.symbol_name = u.symbol_name
         where s.upload_id=u.upload_id
     ),
     upload_stocks_ts as (
         select
             usi.open,
             usi.close,
             usi.high,
             usi.low,
             usi.volume,
             usi.upload_id,
             usi.symbol_id,
             ts.time_serial_id
         from upload_stocks_interval usi
                  join mart.d_time_serial ts
                       on ts.time_serial_time = usi.time
     )


insert into mart.f_price(price_open, price_high, price_low, price_close, price_volume, price_symbol_id, price_time_id)
select  us.open,
        us.high,
        us.low,
        us.close,
        us.volume,
        us.symbol_id,
        us.time_serial_id
from upload_stocks_ts us
where us.time_serial_id not in(
    select f.price_time_id
    from mart.f_price f
    where f.price_symbol_id=us.symbol_id
)