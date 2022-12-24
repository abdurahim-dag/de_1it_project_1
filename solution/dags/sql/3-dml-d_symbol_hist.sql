with upload as (
    select distinct u.symbol_name, u.date, s.symbol_id
    from staging.upload_hist u
             join mart.d_symbol_act s on u.symbol_name=s.symbol_name
    where u.upload_id not in (select upload_id from staging.upload_hist where symbol_name=u.symbol_name and date='{{ds}}' and uploaded is true)
    and u.uploaded is not true
)
insert into mart.d_symbol_hist(symbol_id, symbol_name, symbol_valid_from)
select u.symbol_id, u.symbol_name, u.date
from upload u
where u.symbol_name not in (
    select symbol_name
    from mart.d_symbol_hist
    where symbol_valid_to is null
);
