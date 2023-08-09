# Импортируем необходимое
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

# Настроечные КОНСТАНТЫ
# Настрйока пропуски этапов True, если выполнять этап
LOAD_EGRUL = True     # Загружать ЕГРЮЛ
FILTER_EGRUL = True   # Фильтровать (с записью в БД)
LOAD_VACANCIES = True # Загружать вакансии

# Константы для загрузки файла
URL_EGRUL = "https://ofdata.ru/open-data/download/egrul.json.zip"
PATH_EGRUL = "/home/restwin/hw/egrul.json.zip"

PATH_JSON = "/home/restwin/hw/top_key_skills.json"

# Имя соединения к базе данных в airflow
DB_CONN = 'sqllite_restwin'

# Можно установить нужную сферу, ограничив до нужного уровня
OKVED_ID = '61'

# Константы для загрузки вакансий
API_URL = 'https://api.hh.ru/vacancies'
HEADERS = {'User-Agent': 'Mozilla/5.0'}          # Стандартный фейк агент
API_LIMIT_VACNCY = 100                           # Лимит загрузки, не рекомендуется значение больше 100
http_params = {'text':'python middle developer', # промт для поиска вакансий
              'search_field':'name',
              'per_page': 100,                   # Уменьшить, если API_LIMIT_VACNCY меньше
              'page':0,
              'area':113}                        # Регион поиска

# функции проверки
def need_load_egrul():
    """
    Возвращает для задачи состояние настройки LOAD_EGRUL - о необходимости загрузки ЕГРЮЛ
    """
    return LOAD_EGRUL

def need_filter_egrul():
    """
    Возвращает для задачи состояние настройки FILTER_EGRUL - о необходимости фильтрации ЕГРЮЛ
    """
    return FILTER_EGRUL

def need_load_vacncies():
    """
    Возвращает для задачи состояние настройки LOAD_VACANCIES - о необходимости загрузки вакансий
    """
    return LOAD_VACANCIES

# Омновные функции
def load_egrul_http():
    """    Функция загружает файл ЕГРЮЛ, в соответствии с параметрами URL_EGRUL, PATH_EGRUL
    """
    import urllib.request
    import logging

    logger = logging.getLogger(__name__)
    logger.info(f"Начало работы {__name__}")

    # Пробуем скачать файл
    try:
        urllib.request.urlretrieve(URL_EGRUL, PATH_EGRUL)
        logger.debug(f"Загрузка файла в {PATH_EGRUL} завершена")
    except OSError as err:
        logger.error(f"Не удалось скачать файл. Ошибка: {err}")
        raise
        
    logger.info(f"Конец работы {__name__}")

def filter_egrul():
    """
    Функция читает zip файл из PATH_EGRUL, ищет компании с OKVED_ID и записывает в таблицу 'companies'
    """
    import pandas as pd
    from zipfile import ZipFile,BadZipFile
    import json
    import logging

    logger = logging.getLogger(__name__)
    logger.info(f"Начало работы {__name__}")

    list_companies = []

    # Пробуем отфильтровать егрюл из сжатого файла
    logger.info(f"Читаем файл {PATH_EGRUL}")
    try:
        with ZipFile(PATH_EGRUL) as myzip:    
            for file_okved in myzip.namelist():
                logger.debug(f"Смотрим файл {file_okved}")
                with myzip.open(file_okved) as okved_file:
                    data = json.load(okved_file)
                # Ищем ОКВЭД и добавляем в список
                    for string in data:
                        if 'СвОКВЭД' in string['data']:
                            if 'СвОКВЭДОсн' in string['data']['СвОКВЭД']:
                                okved = string['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД']
                                if okved[:len(OKVED_ID)] == OKVED_ID:
                                    instring = {
                                        "name":string['name'],
                                        "full_name":string['full_name'],
                                        "inn":string['inn'],
                                        "kpp":string['kpp'],
                                        "okved":okved
                                    }
                                    list_companies.append(instring)
    except BadZipFile as error:
        logger.error(f"Файл ЕГРЮЛ по пути {PATH_EGRUL} не(до)загружен. Ошибка: {error}")
        raise

    # Пробуем записать результаты
    logger.info(f"Записываем в БД, таблицу companies")
    try:
        sqlite_hook = SqliteHook(sqlite_conn_id=DB_CONN)
        conn = sqlite_hook.get_conn()

        df = pd.DataFrame(list_companies)
        df.to_sql('companies', conn, if_exists='append', index=False)
    except Exception as err:       
        logger.error(f"Не удалось записать результаты фильтрации в таблицу 'companies'. Ошибка: {err}")
        raise

    logger.info(f"Конец работы {__name__}")

def load_vacncies_hh():
    """
    Функция загружает вакансии с сайта hh.ru по патерну http_params['text':'text'] с территории http_params['area':000] в колличестве API_LIMIT_VACNCY
    """
    import pandas as pd
    import requests
    import asyncio
    from aiohttp import ClientSession, TCPConnector
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info(f"Начало работы {__name__}")

    def get_html(link, url_params = ""):
        """
        Получает страницу с указанного url в формате для дальнейшей обработки

        Args:
            link (str): ссылка, которую нужно вызывать
            url_params (dict): параметры url вызываемой страницы, могут быть опущены. Defaults to "".

        Returns:
            object: контент страницы в формате функции requests.get, нужно обработать в нужном виде
        """

        try:
            #Вызов ресурса
            data = requests.get(
                url = link,
                headers=HEADERS,
                params=url_params
            )
            req_status_code = data.status_code
            req_reason = data.reason

            #Проверка статуса и обрыв в случае ошибки, return на всякий случай
            if req_status_code !=200:
                logger.error(f"При загрузке страницы возникла ошибка. Статус: {req_status_code}, Текст: {req_reason}")
                data.raise_for_status()
                return
        except Exception as e:
                return 
        
        return data

    def get_api_links(api_param):
        """Синхронная (обычная) функция, получающая список вакансий со страницы. Позволяет запускать с разными параметрами страницы, в основном для изменения страницы

        Args:
            api_param (dict): словарь параметров, в котором в отличие от константы меняется страница запуска

        Returns:
            list: Список вакансий
        """

        result = get_html(API_URL, api_param)
        
        if result.json().get('items') == []:
            return

        link_list = []
        for vacancy in result.json().get('items'):
            link_list.append(vacancy['url'].split('?')[0])
        
        return link_list

    async def get_vacncies(url, session):
        """Функция получает в асинхронном режиме ссылку и сессию и передает вакансию в формате kson

        Args:
            url (string): ссылка  на вакансию
            session (object): соедниение с БД в асинронном режима

        Returns:
            json: Вакансия с сайта, полученнная в апи режиме
        """
        async with session.get(url=url) as response:
            req_status_code = response.status
            req_reason = response.reason

            #Проверка статуса и обрыв в случае ошибки, return на всякий случай
            if req_status_code !=200:
                logger.error(f"При загрузке страницы {url} возникла ошибка. Статус: {req_status_code}. Ошибка: {err}")
                response.raise_for_status()
                return

            vacancy_json = await response.json()
            
            return vacancy_json

    async def get_api_vacncies(link_list):
        """Асинхронная функция, которая получает список вакансий, добавляет их в список задач и в асинхронном режиме производит запуск ссылок

        Args:
            link_list (list): Список ссылок с вакансиями

        Returns:
            list: список со словарями вакансии с полями в формате:
            {
            "company_name":company_name,
            "position":position,
            "job_description":job_description,
            "key_skills":key_skills
            }
        """
        # Ограничиваем количество максимальных подключений
        connector = TCPConnector(limit=5)
        async with ClientSession(connector=connector) as session:
            tasks = []
            for links in link_list:
                tasks.append(asyncio.create_task(get_vacncies(links, session)))

            results = await asyncio.gather(*tasks)

        data = []
        logger.debug(f"Парсим результаты загрузки вакансий")
        for result in results:
            #поиск данных
            try:
                company_name = result['employer']['name']
                position = result['name']
                job_description = result['description']
                key_skills = [tag['name'] for tag in result['key_skills']]
            except Exception as e:
                logger.warning(f"При парсинге со страницы {API_URL}/{result['id']} отсутствовало поле. Исключение: {err}")
                return 
            
            if key_skills != []:
                vacancy = {
                    "company_name":company_name,
                    "position":position,
                    "job_description":job_description,
                    "key_skills":key_skills
                }

                data.append(vacancy)
                logger.debug(f"Добавлена вакансия {vacancy['position']}")
            else:
                logger.debug(f"Вакансия {vacancy['position']} не содержит ключевых навыков")
        
        return data
    
    logger.info(f"Получаем список ссылок на вакансии со страницы поиска")
    list_vacancies = []  
    links = get_api_links(http_params)

    if links is None:
        logger.warning('Сайт hh.ru не выдал результатов, измените настройки')
        raise

    logger.info(f"Отправляем список вакансий на получение и парсинг")
    data = asyncio.run(get_api_vacncies(links))

    for item in data:
        if item['key_skills'] == []:
            continue
        list_vacancies.append(item)
    logger.debug(f"Поиск вакансий на сайте hh.ru выполнен.")

    # Пробуем записать результаты
    logger.info(f"Записываем в БД, таблицу key_skills")
    try:
        sqlite_hook = SqliteHook(sqlite_conn_id=DB_CONN)
        conn = sqlite_hook.get_conn()

        df = pd.DataFrame(list_vacancies)
        df['key_skills'] = df['key_skills'].str.join(', ')
        df.to_sql('vacancies', conn, if_exists='append', index=False, dtype={"key_skills": "list"})
    except Exception as err:       
        logger.error(f"Не удалось записать результаты фильтрации в таблицу 'companies'. Ошибка: {err}")
        raise

    logger.info(f"Конец работы {__name__}")

def top_keys():
    """
    Функция сравнивающая компании из ЕГРЮЛ и из вакансий, затем выдывая топ ключевых навыков из вакансий, чьи компании вошли в отфильтрованный список
    """
    import json
    import pandas as pd
    from collections import Counter
    import logging

    logger = logging.getLogger(__name__)  
    logger.info(f"Начало работы {__name__}")

    # Создаем датафреймы из таблиц
    try:
        logger.debug(f"Читаем таблицы БД в датафреймы")
        sqlite_hook = SqliteHook(sqlite_conn_id=DB_CONN)
        conn = sqlite_hook.get_conn()

        df_company = pd.read_sql_query("SELECT * FROM companies", conn)
        df_vacancies = pd.read_sql_query("SELECT * FROM vacancies", conn)

        # Очищаем от правовых форм и запятых (в вакансиях)
        logger.debug(f"Очищаем имена компаний от правовых форм и считаем топ")
        repl_list = '|'.join(['ООО ', 'АО ', 'ПАО '])
        df_vacancies['company_name'] = df_vacancies['company_name'].str.replace(repl_list, '',regex=True, case = False).str.split(r',').str[0]
        df_company['name'] = df_company['name'].str.split(r'"').str[1]
        
        # Ищем совпадения между названиями компаний
        df_vacancies = df_vacancies.assign(exst=df_vacancies['company_name'].isin(df_company['name']).astype(int))
        df_filtred_vac = df_vacancies.loc[df_vacancies['exst'] == 1]

        # Считаем топ и записываем
        list_keys = list(df_filtred_vac['key_skills'])
        top_key_skills = dict(Counter(', '.join(list_keys).split(', ')))
        top_key_skills = dict(sorted(top_key_skills.items(), key=lambda x:x[1], reverse=True))

        logger.debug(f"Пишем результаты в файл {PATH_JSON}")
        with open(PATH_JSON, "w", encoding="utf-8") as outfile:
            json.dump(top_key_skills,outfile,indent=4,ensure_ascii=False)

    except Exception as err:
        logger.error(f"Не удалось записать результаты анализа топ ключевых навыков. Ошибка: {err}")
        raise

    logger.info(f"Конец работы {__name__}")

# Запуск airflow
default_args = {
    'owner': 'knyazev-em'    
}

with DAG(
    dag_id='hwf_topskills_knyazev',
    default_args=default_args,
    description='DAG for donwload okved file',
    start_date=datetime(2023, 8, 7, 8),
    schedule_interval=None
) as dag:
    table1 = SqliteOperator(
        task_id='t_create_table_companies',
        sqlite_conn_id=DB_CONN,
        sql="""
        CREATE TABLE IF NOT EXISTS companies(
        name TEXT,
        full_name TEXT,
        inn TEXT,
        kpp TEXT,
        okved TEXT
        );
        """
        )

    table2 = SqliteOperator(
        task_id='t_create_table_vacancies',
        sqlite_conn_id=DB_CONN,
        sql="""
        CREATE TABLE IF NOT EXISTS vacancies(
        company_name TEXT,
        position TEXT,
        job_description TEXT,
        key_skills TEXT
        );
        """
        )

    egrul1 = ShortCircuitOperator(
        task_id='t_check_need_load_egrul',
        python_callable=need_load_egrul,
        ignore_downstream_trigger_rules=False
        )

    egrul2 = PythonOperator(
        task_id='t_download_egrul',
        python_callable=load_egrul_http
        )

    egrul3 = ShortCircuitOperator(
        task_id='t_check_need_filter_egrul',
        python_callable=need_filter_egrul,
        ignore_downstream_trigger_rules=False,
        trigger_rule='all_done'
        )

    egrul4 = PythonOperator(
        task_id='t_filter_egrul',
        python_callable=filter_egrul
        )

    vacncies1 = ShortCircuitOperator(
        task_id='t_check_need_load_vacncies',
        python_callable=need_load_vacncies,
        ignore_downstream_trigger_rules=False,
        trigger_rule='all_done'
        )
    
    vacncies2 = PythonOperator(
        task_id='t_load_vacncies_hh',
        python_callable=load_vacncies_hh
        )

    top = PythonOperator(
        task_id='t_top_keys',
        python_callable=top_keys,
        trigger_rule='none_failed'
        )
    
    table1 >> table2 >> [egrul1, vacncies1]
    egrul1 >> egrul2 >> egrul3 >> egrul4
    vacncies1 >> vacncies2
    [egrul4, vacncies2] >> top