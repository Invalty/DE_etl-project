create or replace procedure ds.fill_account_turnover_f(
 	i_OnDate DATE) 
language plpgsql
as $$
declare 
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    rows_inserted INTEGER;
begin
    start_time := now();
    
    -- логирование начала
    insert into logs.etl_log (task_id, execution_date, table_name, status, start_time)
    values ('fill_account_turnover_f', now(), 'dm.dm_account_turnover_f', 'started', start_time);
    
    -- очищаем записи при перезапуске
    delete from dm.dm_account_turnover_f where on_date = i_OnDate;

    -- основной запрос с cte
    with
    -- кредитовые обороты по счетам
    credit_turnover as (
        select p.credit_account_rk as account_rk,
            sum(p.credit_amount) as credit_sum,
            a.currency_rk
        from ds.ft_posting_f p
        left join ds.md_account_d a on a.account_rk = p.credit_account_rk
            and i_OnDate between a.data_actual_date and a.data_actual_end_date
        where p.oper_date = i_OnDate
		  and p.credit_account_rk is not null -- счет участвовал в проводке как кредит		 
        group by p.credit_account_rk, a.currency_rk
    ),
    
    -- дебетовые обороты по счетам
    debet_turnover as (
        select p.debet_account_rk as account_rk,
            sum(p.debet_amount) as debet_sum,
            a.currency_rk
        from ds.ft_posting_f p
        left join ds.md_account_d a on a.account_rk = p.debet_account_rk
            and i_OnDate between a.data_actual_date and a.data_actual_end_date
        where p.oper_date = i_OnDate 
		  and p.debet_account_rk is not null
        group by p.debet_account_rk, a.currency_rk
    ),
    
    -- курсы валют на дату
    exchange_rates as (
        select currency_rk, reduced_cource
        from ds.md_exchange_rate_d
        where i_OnDate between data_actual_date and data_actual_end_date
    )
    
    -- собираем всё вместе
	-- coalesce защищает от NULL: подставляет 0 вместо пустых сумм и 1 вместо отсутствующего курса 
    insert into dm.dm_account_turnover_f (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
    select i_OnDate,
        coalesce(c.account_rk, d.account_rk) as account_rk,
        coalesce(c.credit_sum, 0) as credit_amount,
        coalesce(c.credit_sum, 0) * coalesce(er_c.reduced_cource, 1) as credit_amount_rub,
        coalesce(d.debet_sum, 0) as debet_amount,
        coalesce(d.debet_sum, 0) * coalesce(er_d.reduced_cource, 1) as debet_amount_rub
    from credit_turnover c
    full join debet_turnover d using(account_rk)
    left join exchange_rates er_c on er_c.currency_rk = c.currency_rk
    left join exchange_rates er_d on er_d.currency_rk = d.currency_rk;
    
    get diagnostics rows_inserted = row_count; --Сохраняет количество строк, затронутых последним выполненным SQL-запросом
    end_time := now();
    
    -- логирование 
    insert into logs.etl_log (task_id, execution_date, table_name, status, rows_affected, start_time, end_time, duration_seconds)
    values ('fill_account_turnover_f', now(), 'dm.dm_account_turnover_f', 'success', rows_inserted, start_time, end_time, extract(epoch from (end_time - start_time)));

exception when others then
    insert into logs.etl_log (task_id, execution_date, table_name, status, start_time, end_time, error_message)
    values ('fill_account_turnover_f', now(), 'dm.dm_account_turnover_f', 'failed', start_time, now(), sqlerrm);
    raise;
end;
$$;

create or replace procedure ds.fill_account_balance_f(i_OnDate DATE)
language plpgsql
as $$
declare 
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    rows_inserted INTEGER;
    prev_date DATE;
begin
    start_time := now();
    prev_date := i_OnDate - interval '1 day';
    
    -- логирование начала
    insert into logs.etl_log (task_id, execution_date, table_name, status, start_time)
    values ('fill_account_balance_f', now(), 'dm.dm_account_balance_f', 'started', start_time);
    
    -- очищаем записи при перезапуске
    delete from dm.dm_account_balance_f where on_date = i_OnDate;
    
    -- вставляем остатки для всех действующих счетов
    insert into dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
    select 
        i_OnDate, 
        a.account_rk,
        case 
            -- активный счет: остаток = прошлый остаток + дебет - кредит
            when a.char_type = 'А' then 
                coalesce(prev_bal.balance_out, 0) + coalesce(turn.debet_amount, 0) - coalesce(turn.credit_amount, 0)
            -- пассивный счет: остаток = прошлый остаток - дебет + кредит
            when a.char_type = 'П' then 
                coalesce(prev_bal.balance_out, 0) - coalesce(turn.debet_amount, 0) + coalesce(turn.credit_amount, 0)
            else 0
        end as balance_out,
        case 
            when a.char_type = 'А' then 
                coalesce(prev_bal.balance_out_rub, 0) + coalesce(turn.debet_amount_rub, 0) - coalesce(turn.credit_amount_rub, 0)
            when a.char_type = 'П' then 
                coalesce(prev_bal.balance_out_rub, 0) - coalesce(turn.debet_amount_rub, 0) + coalesce(turn.credit_amount_rub, 0)
            else 0
        end as balance_out_rub
    from ds.md_account_d a
    -- остатки с предыдущего дня
    left join dm.dm_account_balance_f prev_bal 
        on prev_bal.account_rk = a.account_rk 
        and prev_bal.on_date = prev_date 
    -- обороты за текущий день
    left join dm.dm_account_turnover_f turn 
        on turn.account_rk = a.account_rk 
        and turn.on_date = i_OnDate
    -- берем только счета, действующие в текущую дату
    where i_OnDate between a.data_actual_date and a.data_actual_end_date;
    
    get diagnostics rows_inserted = row_count;
    end_time := now(); 
    
    -- логирование успеха
    insert into logs.etl_log (task_id, execution_date, table_name, status, rows_affected, start_time, end_time, duration_seconds)
    values ('fill_account_balance_f', now(), 'dm.dm_account_balance_f', 'success', rows_inserted, start_time, end_time, extract(epoch from (end_time - start_time)));
    
exception when others then
    insert into logs.etl_log (task_id, execution_date, table_name, status, start_time, end_time, error_message)
    values ('fill_account_balance_f', now(), 'dm.dm_account_balance_f', 'failed', start_time, now(), sqlerrm);
    raise;
end;
$$;


-- Заполнение витрины остатков начальными данными на 31.12.2017
insert into dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
select '2017-12-31' as on_date, bf.account_rk,  bf.balance_out,           -- остаток в валюте счета
    bf.balance_out * coalesce(er.reduced_cource, 1) as balance_out_rub  -- остаток в рублях
from ds.ft_balance_f bf
-- подтягиваем справочник счетов, чтобы узнать валюту счета (currency_rk)
left join ds.md_account_d a on a.account_rk = bf.account_rk
	and '2017-12-31' between a.data_actual_date and a.data_actual_end_date
-- подтягиваем курс валюты на 31.12.2017
left join ds.md_exchange_rate_d er on a.currency_rk = er.currency_rk
    and '2017-12-31' between er.data_actual_date and er.data_actual_end_date;

-- Проверим количество записей
select count(*) as rows_count
from dm.dm_account_balance_f
where on_date = '2017-12-31';

do $$
declare
    v_date date;
begin
	-- Цикл по всем дням января 2018
    -- generate_series создаёт последовательность дат с '2018-01-01' по '2018-01-31' с шагом 1 день
    for v_date in select generate_series('2018-01-01'::date, '2018-01-31'::date, '1 day')
    loop
        -- рассчитываем обороты за день
        call ds.fill_account_turnover_f(v_date);
        
        -- рассчитываем остатки за день
        call ds.fill_account_balance_f(v_date);
    end loop;
end;
$$;

-- Проверка оборотов за январь
select on_date, count(*) as records_count
from dm.dm_account_turnover_f
where on_date between '2018-01-01' and '2018-01-31'
group by on_date
order by on_date;

-- Проверка остатков за январь
select on_date, count(*) as records_count
from dm.dm_account_balance_f
where on_date between '2018-01-01' and '2018-01-31'
group by on_date
order by on_date;

select task_id, status, rows_affected, 
       start_time, end_time, duration_seconds
from logs.etl_log 
where task_id in ('fill_account_turnover_f', 'fill_account_balance_f')
order by log_id desc;

