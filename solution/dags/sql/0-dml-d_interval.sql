with upload as (
    select distinct u.interval_name
    from staging.upload_hist u
    where u.upload_id not in (select upload_id from staging.upload_hist where date='{{ds}}' and uploaded)
)

insert into mart.d_interval(interval_name)
select interval_name
from upload us
where us.interval_name not in (
    select interval_name
    from mart.d_interval
);

