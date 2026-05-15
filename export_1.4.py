import psycopg2
import csv
from datetime import datetime
import os

def export_f101_to_csv():
    start_time = datetime.now()

    # Параметры подключения к БД
    conn_params = {
        'host': 'localhost',
        'port': 5432,
        'database': 'etl-project',
        'user': 'postgres',
        'password': 'admin'
    }

    conn = None
    cur = None

    try:
        # Подключаемся к БД
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        # Проверяем, есть ли данные в таблице
        cur.execute("SELECT COUNT(*) FROM dm.dm_f101_round_f")
        count = cur.fetchone()[0]
        print(f"Найдено записей в таблице: {count}")

        if count == 0:
            print("Таблица пуста. Экспорт невозможен")
            return

        # Выгружаем данные
        print("Выгрузка данных")
        cur.execute("""
            SELECT * FROM dm.dm_f101_round_f 
            ORDER BY from_date, chapter, ledger_account
        """)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

        output_file = 'dm_f101_round_f.csv'
        print(f"Запись в файл: {output_file}")

        with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(colnames)  # заголовки
            writer.writerows(rows)  # данные

        rows_exported = len(rows)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"[{end_time}] Экспорт завершен")

        # Логирование в БД
        try:
            cur.execute("""
                INSERT INTO logs.etl_log 
                (task_id, execution_date, table_name, status, rows_affected, start_time, end_time, duration_seconds, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                'export_f101_to_csv', datetime.now(), 'dm.dm_f101_round_f', 'SUCCESS',
                rows_exported, start_time, end_time, duration,
                f'Экспорт в файл {output_file}'
            ))
            conn.commit()
        except Exception as log_err:
            print(f"Не удалось записать лог: {log_err}")

    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"[{end_time}] Экспорт завершен с ошибкой")


    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("Соединение с БД закрыто")


if __name__ == "__main__":
    export_f101_to_csv()


