CREATE SCHEMA IF NOT EXISTS logs;
CREATE TABLE IF NOT EXISTS logs.etl_log (
    log_id SERIAL PRIMARY KEY,
    task_id VARCHAR(100),   -- Название задачи
    execution_date TIMESTAMP,  -- Дата запуска DAG
    table_name VARCHAR(100),   -- Таблица назначения
    status VARCHAR(20), -- STARTED, RUNNING, SUCCESS, FAILED, SKIPPED
    rows_read INTEGER,       -- Сколько строк прочитано из CSV
    rows_inserted INTEGER,   -- Сколько вставлено (новых)
    rows_updated INTEGER,     -- Сколько обновлено (upsert)
    rows_skipped INTEGER,      -- Сколько пропущено (ошибки валидации)
    rows_affected INTEGER,     -- Общее количество обработанных строк
    
    -- Производительность
    start_time TIMESTAMP,      -- Время начала задачи
    end_time TIMESTAMP,    -- Время окончания задачи
    duration_seconds NUMERIC(10,3), -- Длительность в сек

    -- Информация об ошибках
    error_message TEXT              -- Текст ошибки
);

