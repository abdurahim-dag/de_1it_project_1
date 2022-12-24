with upload as (
    select distinct u.upload_id, u.interval_name
    from staging.upload_hist u
    where u.upload_id not in (select upload_id from staging.upload_hist where symbol_name=u.symbol_name and date='{{ds}}' and uploaded is true)
    and u.uploaded is not true
),
 upload_stocks as (
     select distinct s.time,
                     u.interval_name
     from staging.stocks s
              join upload u on s.upload_id=u.upload_id
 ),
 time_serial as (
     select time, interval.interval_id
     from upload_stocks us
              join mart.d_interval interval
                   on interval.interval_name = us.interval_name
 )

insert into mart.d_time_serial(time_serial_time, time_serial_interval_id)
select ts.time, ts.interval_id
from time_serial ts
where time not in(
    select dts.time_serial_time
    from mart.d_time_serial dts
    where dts.time_serial_interval_id=ts.interval_id
)