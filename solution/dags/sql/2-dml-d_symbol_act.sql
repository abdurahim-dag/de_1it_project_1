with upload as (
    select distinct u.symbol_name
    from staging.upload_hist u
    where u.upload_id not in (select upload_id from staging.upload_hist where symbol_name=u.symbol_name and date='{{ds}}' and uploaded is true)
    and u.uploaded is not true
)
insert into mart.d_symbol_act(symbol_name)
select u.symbol_name
from upload u
where u.symbol_name not in (
    select symbol_name
    from mart.d_symbol_act
);