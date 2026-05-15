import psycopg2
import csv
from datetime import datetime
import os

def import_f101_from_csv():
    start_time = datetime.now()
    rows_imported = 0
    input_file = 'dm_f101_round_f.csv'

    print(f"[{start_time}] начало импорта")

    # Проверяем, существует ли файл
    if not os.path.exists(input_file):
        print(f"Файл {input_file} не найден!")
        return

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
        print("Подключение к базе данных")
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        print("Подключение успешно")

        # Создаём копию таблицы (если не существует)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f_v2 (
                like dm.dm_f101_round_f including all
            )
        """)
        conn.commit()
        print("Таблица dm.dm_f101_round_f_v2 готова")

        # Очищаем целевую таблицу перед загрузкой
        cur.execute("TRUNCATE TABLE dm.dm_f101_round_f_v2")
        conn.commit()
        print("Таблица очищена")

        # Читаем CSV и вставляем данные
        print(f"Чтение файла: {input_file}")
        with open(input_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f, delimiter=';')
            headers = next(reader)

            print(f"Колонки: {', '.join(headers[:5])}...")

            for row in reader:
                if len(row) != len(headers):
                    print(f"Пропущена строка: несоответствие колонок")
                    continue

                # Преобразуем пустые строки в None
                converted_row = []
                for value in row:
                    if value == '' or value.strip() == '':
                        converted_row.append(None)
                    else:
                        converted_row.append(value)

                placeholders = ','.join(['%s'] * len(converted_row))
                sql = f"""
                    INSERT INTO dm.dm_f101_round_f_v2 ({','.join(headers)})
                    VALUES ({placeholders})
                """
                cur.execute(sql, converted_row)
                rows_imported += 1

        conn.commit()

        cur.execute("SELECT COUNT(*) FROM dm.dm_f101_round_f_v2")
        count = cur.fetchone()[0]

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"[{end_time}] импорт завершен успешно")
        print(f"Загружено строк: {count}")

        # Логирование в БД (
        try:
            cur.execute("""
                INSERT INTO logs.etl_log 
                (task_id, execution_date, table_name, status, rows_affected, start_time, end_time, duration_seconds)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                'import_f101_from_csv', datetime.now(), 'dm.dm_f101_round_f_v2', 'SUCCESS',
                rows_imported, start_time, end_time, duration
            ))
            conn.commit()
            print("Лог успешно записан")
        except Exception as log_err:
            print(f"Не удалось записать лог: {log_err}")

    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"[{end_time}] импорт завершен с ошибкой: {e}")

        # Логирование ошибки
        try:
            if cur:
                cur.execute("""
                    INSERT INTO logs.etl_log 
                    (task_id, execution_date, table_name, status, start_time, end_time, duration_seconds, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    'import_f101_from_csv', datetime.now(), 'dm.dm_f101_round_f_v2', 'FAILED',
                    start_time,  end_time, duration, str(e)
                ))
                conn.commit()
        except Exception as log_err:
            print(f"Не удалось записать лог ошибки: {log_err}")
        raise

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("Соединение с БД закрыто")

if __name__ == "__main__":
    import_f101_from_csv()