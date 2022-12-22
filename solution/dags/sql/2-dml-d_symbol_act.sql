with upload as (
    select distinct u.symbol_name
    from staging.upload_hist u
    where u.upload_id not in (select upload_id from staging.upload_hist where date='{{ds}}')
)
insert into mart.d_symbol_act(symbol_name)
select u.symbol_name
from upload u
where u.symbol_name not in (
    select symbol_name
    from mart.d_symbol_act
);