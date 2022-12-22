update mart.d_symbol_act set symbol_name={new_symbol_name} where symbol_name={old_symbol_name};
update mart.d_symbol_hist
set symbol_valid_to={new_symbol_date}
where symbol_name={new_symbol_name} and symbol_valid_to is null;
insert into mart.d_symbol_hist(symbol_id, symbol_valid_from, symbol_name)
select s_act.symbol_id, {new_symbol_date}, {new_symbol_name}
from mart.d_symbol_act s_act
where s_act.symbol_name={old_symbol_name};
