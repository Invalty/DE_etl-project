-- Создание схем
CREATE SCHEMA IF NOT EXISTS ds;
CREATE SCHEMA IF NOT EXISTS dm;
CREATE SCHEMA IF NOT EXISTS logs;

-- Таблицы детального слоя DS

-- DS.FT_BALANCE_F - остатки по счетам
CREATE TABLE IF NOT EXISTS ds.ft_balance_f (
    on_date DATE NOT NULL,
    account_rk INTEGER NOT NULL,
    currency_rk INTEGER,
    balance_out NUMERIC,
    CONSTRAINT pk_ft_balance_f PRIMARY KEY (on_date, account_rk)
);

-- DS.FT_POSTING_F - проводки
CREATE TABLE IF NOT EXISTS ds.ft_posting_f (
    oper_date DATE NOT NULL,
    credit_account_rk INTEGER NOT NULL,
    debet_account_rk INTEGER NOT NULL,
    credit_amount NUMERIC,
    debet_amount NUMERIC
);

-- DS.MD_ACCOUNT_D - справочник счетов
CREATE TABLE IF NOT EXISTS ds.md_account_d (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE NOT NULL,
    account_rk INTEGER NOT NULL,
    account_number VARCHAR(20) NOT NULL,
    char_type VARCHAR(1) NOT NULL,
    currency_rk INTEGER NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    CONSTRAINT pk_md_account_d PRIMARY KEY (data_actual_date, account_rk)
);

-- DS.MD_CURRENCY_D - справочник валют
CREATE TABLE IF NOT EXISTS ds.md_currency_d (
    currency_rk INTEGER NOT NULL,
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_code VARCHAR(3),
    code_iso_char VARCHAR(3),
    CONSTRAINT pk_md_currency_d PRIMARY KEY (currency_rk, data_actual_date)
);

-- DS.MD_EXCHANGE_RATE_D - курсы валют
CREATE TABLE IF NOT EXISTS ds.md_exchange_rate_d (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_rk INTEGER NOT NULL,
    reduced_cource NUMERIC,
    code_iso_num VARCHAR(3),
    CONSTRAINT pk_md_exchange_rate_d PRIMARY KEY (data_actual_date, currency_rk)
);

-- DS.MD_LEDGER_ACCOUNT_S - справочник балансовых счетов
CREATE TABLE IF NOT EXISTS ds.md_ledger_account_s (
    chapter CHAR(1),
    chapter_name VARCHAR(16),
    section_number INTEGER,
    section_name VARCHAR(22),
    subsection_name VARCHAR(21),
    ledger1_account INTEGER,
    ledger1_account_name VARCHAR(47),
    ledger_account INTEGER NOT NULL,
    ledger_account_name VARCHAR(153),
    characteristic CHAR(1),
    is_resident INTEGER,
    is_reserve INTEGER,
    is_reserved INTEGER,
    is_loan INTEGER,
    is_reserved_assets INTEGER,
    is_overdue INTEGER,
    is_interest INTEGER,
    pair_account VARCHAR(5),
    start_date DATE NOT NULL,
    end_date DATE,
    is_rub_only INTEGER,
    min_term VARCHAR(1),
    min_term_measure VARCHAR(1),
    max_term VARCHAR(1),
    max_term_measure VARCHAR(1),
    ledger_acc_full_name_translit VARCHAR(1),
    is_revaluation VARCHAR(1),
    is_correct VARCHAR(1),
    CONSTRAINT pk_md_ledger_account_s PRIMARY KEY (ledger_account, start_date)
);

-- Таблицы слоя DM (витрины)

-- DM.DM_ACCOUNT_TURNOVER_F - витрина оборотов
CREATE TABLE IF NOT EXISTS dm.dm_account_turnover_f (
    on_date DATE NOT NULL,
    account_rk INTEGER NOT NULL,
    credit_amount NUMERIC(23,8),
    credit_amount_rub NUMERIC(23,8),
    debet_amount NUMERIC(23,8),
    debet_amount_rub NUMERIC(23,8),
    CONSTRAINT pk_dm_account_turnover_f PRIMARY KEY (on_date, account_rk)
);

-- DM.DM_ACCOUNT_BALANCE_F - витрина остатков
CREATE TABLE IF NOT EXISTS dm.dm_account_balance_f (
    on_date DATE NOT NULL,
    account_rk INTEGER NOT NULL,
    balance_out NUMERIC(23,8),
    balance_out_rub NUMERIC(23,8),
    CONSTRAINT pk_dm_account_balance_f PRIMARY KEY (on_date, account_rk)
);

-- DM.DM_F101_ROUND_F - форма 101
CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f (
    from_date DATE,
    to_date DATE,
    chapter CHAR(1),
    ledger_account CHAR(5),
    characteristic CHAR(1),
    balance_in_rub NUMERIC(23,8),
    r_balance_in_rub NUMERIC(23,8),
    balance_in_val NUMERIC(23,8),
    r_balance_in_val NUMERIC(23,8),
    balance_in_total NUMERIC(23,8),
    r_balance_in_total NUMERIC(23,8),
    turn_deb_rub NUMERIC(23,8),
    r_turn_deb_rub NUMERIC(23,8),
    turn_deb_val NUMERIC(23,8),
    r_turn_deb_val NUMERIC(23,8),
    turn_deb_total NUMERIC(23,8),
    r_turn_deb_total NUMERIC(23,8),
    turn_cre_rub NUMERIC(23,8),
    r_turn_cre_rub NUMERIC(23,8),
    turn_cre_val NUMERIC(23,8),
    r_turn_cre_val NUMERIC(23,8),
    turn_cre_total NUMERIC(23,8),
    r_turn_cre_total NUMERIC(23,8),
    balance_out_rub NUMERIC(23,8),
    r_balance_out_rub NUMERIC(23,8),
    balance_out_val NUMERIC(23,8),
    r_balance_out_val NUMERIC(23,8),
    balance_out_total NUMERIC(23,8),
    r_balance_out_total NUMERIC(23,8)
);
