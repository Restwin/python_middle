import sqlite3

DB_NAME = '/home/restwin/hw/hw.db'

companies = 'companies'
del_companies = True

vacancies = 'vacancies'
del_vacancies = True

def del_tables():
    """Функция удаления всех известных таблиц в базе данных
    """
    print(f'Открываем содениене с БД {DB_NAME}')
    
    try:
        conn = sqlite3.connect(DB_NAME) #Имя установлено в constants.py
        cur = conn.cursor()
        print(f'Соединение с БД {DB_NAME} установлено')

    except Exception as e:
        print(f"При попытке установить соединение к БД {DB_NAME} произошла ошибка. Исключение: {e}")
        raise

    print(f'Удаляем таблицы в БД {DB_NAME}')  
    try:
        if del_companies:
            print(f'Удаляем таблицу {companies}')
            cur.execute(f"DROP TABLE IF EXISTS {companies};")
            conn.commit()
            print(f'Таблица {companies} удалена')

        if del_vacancies:
            print(f'Удаляем таблицу {vacancies}')
            cur.execute(f"DROP TABLE IF EXISTS {vacancies};")
            conn.commit()
            print(f'Таблица {vacancies} удалена')
            print(f'Все таблицы в БД {DB_NAME} удалены')

    except Exception as e:
        print(f"При попытке удалить таблицы в БД {DB_NAME} произошла ошибка. Исключение: {e}")
        if conn:
            conn.close()
        print(f'Соединение с БД {DB_NAME} закрыто')
        raise    

    print(f'Закрываем содениене с БД {DB_NAME}')    
    try:
        if conn:
            conn.close()
        print(f'Соединение с БД {DB_NAME} закрыто')
    except Exception as e:
        print(f"При попытке закрыть соединение к БД {DB_NAME} произошла ошибка. Исключение: {e}")


del_tables()