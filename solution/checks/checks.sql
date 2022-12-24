-- Контроль качества.

-- Сверка тикеров.
select u.symbol_name
from staging.upload_hist u
group by u.symbol_name;

select sa.symbol_name
from mart.d_symbol_act sa
group by sa.symbol_name;

-- Количественная сверка
(with symbols as (
select u.symbol_name, u.upload_id
from staging.upload_hist u
group by u.symbol_name, u.upload_id
)

select count(*), symbols.symbol_name, 'staging' as "layer"
from staging.stocks s
join symbols on s.upload_id=symbols.upload_id
group by symbol_name)
union all
(
with symbols as (
select sa.symbol_name, sa.symbol_id
from mart.d_symbol_act sa
group by sa.symbol_name, sa.symbol_id
)

select count(*), symbols.symbol_name, 'core' as "layer"
from mart.f_price f
join symbols on f.price_symbol_id=symbols.symbol_id
group by symbol_name);

-- Сверка по агрегированным данным.
(with symbols as (
select u.symbol_name, u.upload_id
from staging.upload_hist u
group by u.symbol_name, u.upload_id
)

select
    sum(s.close) as close,
    sum(s.open) as open,
    sum(s.low) as low,
    sum(s.high) as high,
    sum(s.volume) as volume,
    s.time::date as "date",
    symbols.symbol_name,
    'staging' as "layer"
from staging.stocks s
join symbols on s.upload_id=symbols.upload_id
group by s.time::date,symbol_name)
union all
(
with symbols as (
select sa.symbol_name, sa.symbol_id
from mart.d_symbol_act sa
group by sa.symbol_name, sa.symbol_id
)

select
    sum(f.price_close) as close,
    sum(f.price_open) as open,
    sum(f.price_low) as low,
    sum(f.price_high) as high,
    sum(f.price_volume) as volume,
    dts.time_serial_time::date as "date",
    symbols.symbol_name,
    'core' as "layer"
from mart.f_price f
join symbols on f.price_symbol_id=symbols.symbol_id
join mart.d_time_serial dts on dts.time_serial_id = f.price_time_id
group by symbols.symbol_name, dts.time_serial_time::date)
order by symbol_name, date;