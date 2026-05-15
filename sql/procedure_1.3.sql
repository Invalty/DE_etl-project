select count(*) from  dm.dm_f101_round_f;

create or replace procedure dm.fill_f101_round_f(i_OnDate DATE)
language plpgsql
as $$
declare 
    _start_time TIMESTAMP;
    _end_time TIMESTAMP;
    _rows_inserted INTEGER;
    _from_date DATE;
    _to_date DATE;
    _prev_date DATE;

begin
    _start_time := now();
    
    -- отчётный период: i_OnDate = первый день следующего месяца
    _from_date := (i_OnDate - interval '1 month')::date;
    _to_date := (i_OnDate - interval '1 day')::date;
    _prev_date := (i_OnDate - interval '1 month' - interval '1 day')::date;
    
    -- логирование начала
    insert into logs.etl_log (task_id, execution_date, table_name, status, start_time)
    values ('fill_f101_round_f', now(), 'dm.dm_f101_round_f', 'started', _start_time);
    
    -- удаляем старые записи за этот период
    delete from dm.dm_f101_round_f where from_date = _from_date and to_date = _to_date;
    
    -- основной запрос
    insert into dm.dm_f101_round_f (
        from_date, to_date, chapter, ledger_account, characteristic,
        balance_in_rub, balance_in_val, balance_in_total,
        turn_deb_rub, turn_deb_val, turn_deb_total,
        turn_cre_rub, turn_cre_val, turn_cre_total,
        balance_out_rub, balance_out_val, balance_out_total
    )
    with 
	-- Берём все счета, активные в отчётном периоде по первым 5 цифрам (балансовый счёт 2 порядка)
    accounts_grouped as (
        select left(a.account_number, 5) as ledger_account,
            a.char_type as characteristic,
            a.currency_code,
            a.account_rk
        from ds.md_account_d a
        where _from_date between a.data_actual_date and a.data_actual_end_date
    ),
    -- входящие остатки (на день перед отчётным периодом)
    balances_in as (
        select b.account_rk, b.balance_out_rub
        from dm.dm_account_balance_f b
        where b.on_date = _prev_date
    ),
    -- Обороты за отчет период - Суммируем дебетовые и кредитовые обороты 
    turnovers as (
        select t.account_rk,
            sum(t.debet_amount_rub) as debet_sum,
            sum(t.credit_amount_rub) as credit_sum
        from dm.dm_account_turnover_f t
        where t.on_date between _from_date and _to_date
        group by t.account_rk
    ),
    -- исходящие остатки (на день отчётного периода)
    balances_out as (
        select b.account_rk, b.balance_out_rub
        from dm.dm_account_balance_f b
        where b.on_date = _to_date
    )
    
    select 
        _from_date as from_date,
        _to_date as to_date,
        ls.chapter,
        ag.ledger_account,
        ag.characteristic,
        
        -- рубли
        sum(bi.balance_out_rub) filter (where ag.currency_code in ('810', '643')) as balance_in_rub,
        sum(t.debet_sum) filter (where ag.currency_code in ('810', '643')) as turn_deb_rub,
        sum(t.credit_sum) filter (where ag.currency_code in ('810', '643')) as turn_cre_rub,
        sum(bo.balance_out_rub) filter (where ag.currency_code in ('810', '643')) as balance_out_rub,
        
        -- валюта
        sum(bi.balance_out_rub) filter (where ag.currency_code not in ('810', '643')) as balance_in_val,
        sum(t.debet_sum) filter (where ag.currency_code not in ('810', '643')) as turn_deb_val,
        sum(t.credit_sum) filter (where ag.currency_code not in ('810', '643')) as turn_cre_val,
        sum(bo.balance_out_rub) filter (where ag.currency_code not in ('810', '643')) as balance_out_val,
        
        -- total
        sum(bi.balance_out_rub) as balance_in_total,
        sum(t.debet_sum) as turn_deb_total,
        sum(t.credit_sum) as turn_cre_total,
        sum(bo.balance_out_rub) as balance_out_total
        
    from accounts_grouped ag
    left join balances_in bi on bi.account_rk = ag.account_rk
    left join turnovers t on t.account_rk = ag.account_rk
    left join balances_out bo on bo.account_rk = ag.account_rk
    left join ds.md_ledger_account_s ls on ls.ledger_account = ag.ledger_account::int
        and _from_date between ls.start_date and coalesce(ls.end_date, '2999-12-31')
    where bi.balance_out_rub is not null 
       or t.debet_sum is not null 
       or t.credit_sum is not null 
       or bo.balance_out_rub is not null
    group by ls.chapter, ag.ledger_account, ag.characteristic;
    
    get diagnostics _rows_inserted = row_count;
    _end_time := now();
    
    insert into logs.etl_log (task_id, execution_date, table_name, status, rows_affected, start_time, end_time, duration_seconds)
    values ('fill_f101_round_f', now(), 'dm.dm_f101_round_f', 'success', _rows_inserted, _start_time, _end_time, extract(epoch from (_end_time - _start_time)));
    
exception when others then
    insert into logs.etl_log (task_id, execution_date, table_name, status, start_time, end_time, error_message)
    values ('fill_f101_round_f', now(), 'dm.dm_f101_round_f', 'failed', _start_time, now(), sqlerrm);
    raise;
end;
$$;

-- вызываем процедуру с датой 1 февраля 2018 (первый день следующего месяца)
call dm.fill_f101_round_f('2018-02-01');

-- проверим результат
select * from dm.dm_f101_round_f;

-- проверим логи
select * from logs.etl_log where task_id = 'fill_f101_round_f' order by log_id desc;
