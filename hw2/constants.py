#Constants

#common
DB_NAME = 'hw.db'                       # Имя файла базы данных
HEADERS = {'User-Agent': 'Mozilla/5.0'} # Стандартный фейк агент
TIME_SLEEP = 1                          # Можно увеличить, Если сразу банят, или уменьшить до .5
PATH_LOG = 'logs/logger.conf'           # Место расположения конфига для логов
# параметры url для всех одинаковые
http_params = {'text':'python middle developer',
              'search_field':'name',
              'per_page': 100,
              'page':0,
              'area':113}

#for api
API_URL = 'https://api.hh.ru/vacancies'
API_DB_TABLE = 'vacancies_api'
API_LIMIT_VACNCY = 100 # не рекомендуется значение больше 100

#for pars
PARS_URL_SEARCH = "https://hh.ru/search/vacancy"
PARS_DB_TABLE = 'vacancies_pars'
PARS_LIMIT_VACNCY = 100

#закомментировано за ненадобностью, но хороший вариант для нескольких территорий
#"""area_list = [1,2] #Список городов (113 - Россия,1 - Москва, 2 - Санкт-Петербург)"""
