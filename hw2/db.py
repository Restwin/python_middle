from constants import *
import logging
import logging.config
import sqlite3
import pandas as pd

#Настраиваем логгер
logging.config.fileConfig(fname=PATH_LOG, disable_existing_loggers=False)
logger = logging.getLogger('dbLogger')

def write_db(data, table):
    """Функция пишет ифнормацию из списка со словарями в базу данных через функционал pandas

    Args:
        data (list): Список со словарями, содержащие поля company_name TEXT, position TEXT, job_description TEXT, key_skills TEXT
        table (str): имя таблицы, которая будет создана (при отсутствии), и куда будет записана информация
    """
    
    logger.debug(f'Открываем содениене с БД {DB_NAME}')
    
    try:
        conn = sqlite3.connect(DB_NAME) #Имя установлено в constants.py
        cur = conn.cursor()
        logger.debug(f'Соединение с БД {DB_NAME} установлено')
    except Exception as e:
        logger.error(f"При попытке установить соединение к БД {DB_NAME} произошла ошибка. Исключение: {e}")
        raise
    

    #Проверка и создание таблицы
    if not check_table(cur, conn, table):
        create_table(cur, conn, table)

    logger.info(f'Пишем переданную информацию в базу данных, таблицу {table}')
    # Создаем пандосовский датафрейм, который затем сохраняем в БД в таблицу
    try:
        df = pd.DataFrame(data)
        df['key_skills'] = df['key_skills'].str.join(', ')
        df.to_sql(table, conn, if_exists='append', index=False, dtype={"key_skills": "list"})
        logger.info(f'Информация в базу данных, таблицу {table} успешно записана.')
    except Exception as e:
        logger.error(f"При попытке передать информацию в базу данных, таблицу {table}' произошла ошибка. Исключение: {e}")
        if cur:
            cur.close()
        if conn:
            conn.close()
        logger.debug(f'Соединение с БД {DB_NAME} закрыто')
        raise
        

    logger.debug(f'Закрываем содениене с БД {DB_NAME}')    
    try:
        if cur:
            cur.close()
        if conn:
            conn.close()
        logger.debug(f'Соединение с БД {DB_NAME} закрыто')
    except Exception as e:
        logger.error(f"При попытке закрыть соединение к БД {DB_NAME} произошла ошибка. Исключение: {e}")
    

def check_table(cur, conn, name):
    """Функция првоеряет наличие таблциы в Базе данных

    Args:
        cur (object): объект курсора соединения c базой данных
        conn (object): объект соединения с базой данных
        name (str): имя проверяемой таблицы

    Returns:
        Boolean: Ответ о наличии или отсутствия таблицы в виде 1/0
    """
    logger.debug(f'Проверяем наличие таблицы {name}')
    try:
        cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{name}';")
        result = cur.fetchone()
    except Exception as e:
        logger.error(f"При попытке проверки наличия таблицы {name} произошла ошибка. Исключение: {e}")
        if cur:
            cur.close()
        if conn:
            conn.close()
        logger.debug(f'Соединение с БД {DB_NAME} закрыто')
        raise

    if result is not None and result[0]:
        logger.debug(f'Таблица {name} есть')
    else:
        logger.debug(f'Таблица {name} отсутствует')

    return result

def create_table(cur, conn, name):
    """Функция создает таблциу в Базе данных

    Args:
        cur (object): объект курсора соединения c базой данных
        conn (object): объект соединения с базой данных
        name (str): имя создаваемой таблицы
    """
    logger.debug(f'Создаем таблицу (если отсутствует) {name}')
    try:
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {name}(
        company_name TEXT,
        position TEXT,
        job_description TEXT,
        key_skills TEXT
        );
        """)
        conn.commit()
    except Exception as e:
        logger.error(f"При попытке создания таблицы {name} произошла ошибка. Исключение: {e}")
        if cur:
            cur.close()
        if conn:
            conn.close()
        logger.debug(f'Соединение с БД {DB_NAME} закрыто')
        raise

    logger.debug(f'Таблица {name} создана')

def del_tables():
    """Функция удаления всех известных таблиц в базе данных (для парсинга и для апи)
    """
    logger.debug(f'Открываем содениене с БД {DB_NAME}')
    
    try:
        conn = sqlite3.connect(DB_NAME) #Имя установлено в constants.py
        cur = conn.cursor()
        logger.debug(f'Соединение с БД {DB_NAME} установлено')

    except Exception as e:
        logger.error(f"При попытке установить соединение к БД {DB_NAME} произошла ошибка. Исключение: {e}")
        raise

    logger.info(f'Удаляем таблицы в БД {DB_NAME}')  
    try:
        logger.debug(f'Удаляем таблицу {PARS_DB_TABLE}')
        cur.execute(f"DROP TABLE IF EXISTS {PARS_DB_TABLE};")
        conn.commit()
        logger.debug(f'Таблица {PARS_DB_TABLE} удалена')

        logger.debug(f'Удаляем таблицу {API_DB_TABLE}')
        cur.execute(f"DROP TABLE IF EXISTS {API_DB_TABLE};")
        conn.commit()
        logger.debug(f'Таблица {API_DB_TABLE} удалена')
        logger.info(f'Все таблицы в БД {DB_NAME} удалены')
    except Exception as e:
        logger.error(f"При попытке удалить таблицы в БД {DB_NAME} произошла ошибка. Исключение: {e}")
        if cur:
            cur.close()
        if conn:
            conn.close()
        logger.debug(f'Соединение с БД {DB_NAME} закрыто')
        raise    

    logger.debug(f'Закрываем содениене с БД {DB_NAME}')    
    try:
        if cur:
            cur.close()
        if conn:
            conn.close()
        logger.debug(f'Соединение с БД {DB_NAME} закрыто')
    except Exception as e:
        logger.error(f"При попытке закрыть соединение к БД {DB_NAME} произошла ошибка. Исключение: {e}")