from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import csv
import time
import logging

logger = logging.getLogger(__name__)

# Функция для логирования
def write_log(task_id, table_name, status, start_time, end_time=None,
              rows_read=None, rows_inserted=None, rows_updated=None,
              rows_skipped=None, error_message=None):
    hook = PostgresHook(postgres_conn_id='etl-project')
    rows_affected = (rows_inserted or 0) + (rows_updated or 0)

    sql = """
        INSERT INTO logs.etl_log 
        (task_id, execution_date, table_name, status, 
         rows_read, rows_inserted, rows_updated, rows_skipped, rows_affected,
         start_time, end_time, duration_seconds, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    duration = None
    if end_time and start_time:
        duration = (end_time - start_time).total_seconds()

    hook.run(sql, parameters=(
        task_id, datetime.now(), table_name, status,
        rows_read, rows_inserted, rows_updated, rows_skipped, rows_affected,
        start_time, end_time or datetime.now(), duration, error_message
    ))

# Функция для определения дат и перевод в норм вид
def parse_date(date_str):
    if not date_str or str(date_str).strip() == '':
        return None
    date_str = str(date_str).strip()
    formats = ['%d.%m.%Y', '%d-%m-%Y']  # В файле ft_balance_f формат с точкой, ft_posting_f формат dd-mm-YY,
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date() # Переведем их в обычный формат Date YY-mm-dd
        except (ValueError, TypeError):
            continue
    return None


# Функции для загрузки файлов
def load_ft_balance_f(**context):
    task_id = 'load_ft_balance_f'
    table_name = 'ds.ft_balance_f'
    start_time = datetime.now()
    hook = PostgresHook(postgres_conn_id='etl-project')

    try:
        # Логируем начало загрузки
        write_log(task_id, table_name, 'STARTED', start_time)

        # Путь к CSV-файлу (внутри контейнера Docker)
        csv_path = '/opt/airflow/files/ft_balance_f.csv'
        rows_read = 0
        rows_inserted = 0

        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                rows_read += 1
                on_date = parse_date(row['ON_DATE'])

                # SQL-запрос с upsert (обновление или вставка)
                sql = """
                    INSERT INTO ds.ft_balance_f (on_date, account_rk, currency_rk, balance_out)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (on_date, account_rk) 
                    DO UPDATE SET balance_out = EXCLUDED.balance_out
                """
                hook.run(sql, parameters=(
                    on_date, int(row['ACCOUNT_RK']),
                    int(row['CURRENCY_RK']) if row.get('CURRENCY_RK') else None,
                    float(row['BALANCE_OUT']) if row['BALANCE_OUT'] else None
                ))
                rows_inserted += 1

        time.sleep(5)
        write_log(task_id, table_name, 'SUCCESS', start_time,
                  datetime.now(), rows_read, rows_inserted)
        logger.info(f"{table_name}: загружено {rows_read} строк")

    except Exception as e:
        write_log(task_id, table_name, 'FAILED', start_time,
                  datetime.now(), error_message=str(e))
        raise


def load_ft_posting_f(**context):
    task_id = 'load_ft_posting_f'
    table_name = 'ds.ft_posting_f'
    start_time = datetime.now()
    hook = PostgresHook(postgres_conn_id='etl-project')

    try:
        write_log(task_id, table_name, 'STARTED', start_time)
        hook.run("TRUNCATE TABLE ds.ft_posting_f")

        csv_path = '/opt/airflow/files/ft_posting_f.csv'
        rows_read = 0

        # Получаем 1 соединение на весь цикл
        conn = hook.get_conn()
        cursor = conn.cursor()

        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f, delimiter=';')

            for row in reader:
                rows_read += 1

                cursor.execute("""
                    INSERT INTO ds.ft_posting_f 
                    (oper_date, credit_account_rk, debet_account_rk, credit_amount, debet_amount)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    parse_date(row['OPER_DATE']),
                    int(row['CREDIT_ACCOUNT_RK']),
                    int(row['DEBET_ACCOUNT_RK']),
                    float(row['CREDIT_AMOUNT']),
                    float(row['DEBET_AMOUNT'])
                ))

                # Фиксируем каждые 5000 строк (а не после каждой)
                if rows_read % 5000 == 0:
                    conn.commit()
                    logger.info(f"Загружено {rows_read} строк...")

            conn.commit()  # Фиксируем остатки

        cursor.close()
        conn.close()

        time.sleep(5)
        write_log(task_id, table_name, 'SUCCESS', start_time,
                  datetime.now(), rows_read, rows_read)
        logger.info(f"{table_name}: загружено {rows_read} строк")

    except Exception as e:
        write_log(task_id, table_name, 'FAILED', start_time,
                  datetime.now(), error_message=str(e))
        raise

def load_md_account_d(**context):
    task_id = 'load_md_account_d'
    table_name = 'ds.md_account_d'
    start_time = datetime.now()
    hook = PostgresHook(postgres_conn_id='etl-project')

    try:
        write_log(task_id, table_name, 'STARTED', start_time)

        csv_path = '/opt/airflow/files/md_account_d.csv'
        rows_read = 0
        rows_inserted = 0

        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                rows_read += 1

                sql = """
                    INSERT INTO ds.md_account_d 
                    (data_actual_date, data_actual_end_date, account_rk, account_number, 
                     char_type, currency_rk, currency_code)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (data_actual_date, account_rk) 
                    DO UPDATE SET 
                        data_actual_end_date = EXCLUDED.data_actual_end_date,
                        account_number = EXCLUDED.account_number,
                        char_type = EXCLUDED.char_type,
                        currency_rk = EXCLUDED.currency_rk,
                        currency_code = EXCLUDED.currency_code
                """
                hook.run(sql, parameters=(
                    row['DATA_ACTUAL_DATE'],
                    row['DATA_ACTUAL_END_DATE'],
                    int(row['ACCOUNT_RK']),
                    row['ACCOUNT_NUMBER'],
                    row['CHAR_TYPE'],
                    int(row['CURRENCY_RK']),
                    row['CURRENCY_CODE']
                ))
                rows_inserted += 1

        time.sleep(5)
        write_log(task_id, table_name, 'SUCCESS', start_time,
                  datetime.now(), rows_read, rows_inserted)

    except Exception as e:
        write_log(task_id, table_name, 'FAILED', start_time,
                  datetime.now(), error_message=str(e))
        raise


def load_md_currency_d(**context):
    task_id = 'load_md_currency_d'
    table_name = 'ds.md_currency_d'
    start_time = datetime.now()
    hook = PostgresHook(postgres_conn_id='etl-project')

    try:
        write_log(task_id, table_name, 'STARTED', start_time)

        csv_path = '/opt/airflow/files/md_currency_d.csv'
        rows_read = 0
        rows_inserted = 0

        # необходимо сменить кодировку
        with open(csv_path, 'r', encoding='latin-1') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                rows_read += 1

                sql = """
                    INSERT INTO ds.md_currency_d 
                    (currency_rk, data_actual_date, data_actual_end_date, currency_code, code_iso_char)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (currency_rk, data_actual_date) 
                    DO UPDATE SET 
                        data_actual_end_date = EXCLUDED.data_actual_end_date,
                        currency_code = EXCLUDED.currency_code,
                        code_iso_char = EXCLUDED.code_iso_char
                """
                hook.run(sql, parameters=(
                    int(row['CURRENCY_RK']),
                    row['DATA_ACTUAL_DATE'],
                    row['DATA_ACTUAL_END_DATE'],
                    row.get('CURRENCY_CODE'),
                    row.get('CODE_ISO_CHAR')
                ))
                rows_inserted += 1

        time.sleep(5)
        write_log(task_id, table_name, 'SUCCESS', start_time,
                  datetime.now(), rows_read, rows_inserted)

    except Exception as e:
        write_log(task_id, table_name, 'FAILED', start_time,
                  datetime.now(), error_message=str(e))
        raise


def load_md_exchange_rate_d(**context):
    task_id = 'load_md_exchange_rate_d'
    table_name = 'ds.md_exchange_rate_d'
    start_time = datetime.now()
    hook = PostgresHook(postgres_conn_id='etl-project')

    try:
        write_log(task_id, table_name, 'STARTED', start_time)

        csv_path = '/opt/airflow/files/md_exchange_rate_d.csv'
        rows_read = 0
        rows_inserted = 0

        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                rows_read += 1

                sql = """
                    INSERT INTO ds.md_exchange_rate_d 
                    (data_actual_date, data_actual_end_date, currency_rk, reduced_cource, code_iso_num)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (data_actual_date, currency_rk) 
                    DO UPDATE SET 
                        data_actual_end_date = EXCLUDED.data_actual_end_date,
                        reduced_cource = EXCLUDED.reduced_cource,
                        code_iso_num = EXCLUDED.code_iso_num
                """
                hook.run(sql, parameters=(
                    row['DATA_ACTUAL_DATE'],
                    row['DATA_ACTUAL_END_DATE'],
                    int(row['CURRENCY_RK']),
                    float(row['REDUCED_COURCE']) if row.get('REDUCED_COURCE') else None,
                    row.get('CODE_ISO_NUM')
                ))
                rows_inserted += 1

        time.sleep(5)
        write_log(task_id, table_name, 'SUCCESS', start_time,
                  datetime.now(), rows_read, rows_inserted)

    except Exception as e:
        write_log(task_id, table_name, 'FAILED', start_time,
                  datetime.now(), error_message=str(e))
        raise

def load_md_ledger_account_s(**context):
    task_id = 'load_md_ledger_account_s'
    table_name = 'ds.md_ledger_account_s'
    start_time = datetime.now()
    hook = PostgresHook(postgres_conn_id='etl-project')

    try:
        write_log(task_id, table_name, 'STARTED', start_time)

        csv_path = '/opt/airflow/files/md_ledger_account_s.csv'
        rows_read = 0
        rows_inserted = 0

        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                rows_read += 1

                sql = """
                    INSERT INTO ds.md_ledger_account_s 
                    (chapter, chapter_name, section_number, section_name, subsection_name,
                     ledger1_account, ledger1_account_name, ledger_account, ledger_account_name,
                     characteristic, is_resident, is_reserve, is_reserved, is_loan,
                     is_reserved_assets, is_overdue, is_interest, pair_account,
                     start_date, end_date, is_rub_only, min_term, min_term_measure,
                     max_term, max_term_measure, ledger_acc_full_name_translit,
                     is_revaluation, is_correct)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ledger_account, start_date) 
                    DO UPDATE SET 
                        end_date = EXCLUDED.end_date,
                        ledger_account_name = EXCLUDED.ledger_account_name,
                        chapter = EXCLUDED.chapter
                """
                hook.run(sql, parameters=(
                    row.get('CHAPTER'), row.get('CHAPTER_NAME'), row.get('SECTION_NUMBER'),
                    row.get('SECTION_NAME'), row.get('SUBSECTION_NAME'), row.get('LEDGER1_ACCOUNT'),
                    row.get('LEDGER1_ACCOUNT_NAME'), int(row['LEDGER_ACCOUNT']), row.get('LEDGER_ACCOUNT_NAME'),
                    row.get('CHARACTERISTIC'), row.get('IS_RESIDENT'), row.get('IS_RESERVE'),
                    row.get('IS_RESERVED'), row.get('IS_LOAN'), row.get('IS_RESERVED_ASSETS'),
                    row.get('IS_OVERDUE'), row.get('IS_INTEREST'), row.get('PAIR_ACCOUNT'),
                    row['START_DATE'], row['END_DATE'], row.get('IS_RUB_ONLY'), row.get('MIN_TERM'),
                    row.get('MIN_TERM_MEASURE'), row.get('MAX_TERM'), row.get('MAX_TERM_MEASURE'),
                    row.get('LEDGER_ACC_FULL_NAME_TRANSLIT'), row.get('IS_REVALUATION'), row.get('IS_CORRECT')
                ))
                rows_inserted += 1

        time.sleep(5)
        write_log(task_id, table_name, 'SUCCESS', start_time,
                  datetime.now(), rows_read, rows_inserted)

    except Exception as e:
        write_log(task_id, table_name, 'FAILED', start_time,
                  datetime.now(), error_message=str(e))
        raise



# определение саого Dag
default_args = {
    'owner': 'bank_etl',
    'start_date': datetime(2026, 5, 1),
    'retries': 1,
}

with DAG(
        dag_id='etl_load_csv_dag',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
) as dag:
    load_ft_balance = PythonOperator(
        task_id='load_ft_balance_f',
        python_callable=load_ft_balance_f
    )

    load_ft_posting = PythonOperator(
        task_id='load_ft_posting_f',
        python_callable=load_ft_posting_f
    )

    load_md_account = PythonOperator(
        task_id='load_md_account_d',
        python_callable=load_md_account_d
    )

    load_md_currency = PythonOperator(
        task_id='load_md_currency_d',
        python_callable=load_md_currency_d
    )

    load_md_exchange_rate = PythonOperator(
        task_id='load_md_exchange_rate_d',
        python_callable=load_md_exchange_rate_d
    )

    load_md_ledger_account = PythonOperator(
        task_id='load_md_ledger_account_s',
        python_callable=load_md_ledger_account_s
    )

    [load_ft_balance, load_ft_posting, load_md_account,
     load_md_currency, load_md_exchange_rate, load_md_ledger_account]