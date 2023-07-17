from constants import *
from htmlgetter import get_html
import logging
import logging.config
import asyncio
from aiohttp import ClientSession, TCPConnector


#Настраиваем логгер
logging.config.fileConfig(fname=PATH_LOG, disable_existing_loggers=False)
logger = logging.getLogger('apiLogger')

async def get_vacncies(url, session):
    """Функция получает в асинхронном режиме ссылку и сессию и передает вакансию в формате kson

    Args:
        url (string): ссылка  на вакансию
        session (object): соедниение с БД в асинронном режима

    Returns:
        json: Вакансия с сайта, полученнная в апи режиме
    """
    logging.debug(f"Начата загрузка вакансии {url}")

    async with session.get(url=url) as response:
        req_status_code = response.status
        req_reason = response.reason

        #Проверка статуса и обрыв в случае ошибки, return на всякий случай
        if req_status_code !=200:
            logger.error(f"При загрузке страницы возникла ошибка. Статус: {req_status_code}, Текст: {req_reason}")
            response.raise_for_status()
            return

        vacancy_json = await response.json()
        logging.debug(f"Завершена загрузка вакансии {url}")
        
        # if len(vacancy_json['key_skills']['name']) != 0:
        return vacancy_json
            


async def get_api_vacncies(link_list):
    """Асинхронная функция, которая получает список вакансий, добавляет их в список задач и в асинхронном режиме производит запуск ссылок

    Args:
        link_list (list): Список ссылок с вакансиями

    Returns:
        dict: словарь вакансии с полями в формате:
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
    for result in results:
        #поиск данных
        try:
            company_name = result['employer']['name']
            position = result['name']
            job_description = result['description']
            key_skills = [tag['name'] for tag in result['key_skills']]
        except Exception as e:
            logger.error(f"При парсинге со страницы {API_URL}/{result['id']} отсутствовало поле. Исключение: {e}")
            return 
        
        if key_skills != []:
            vacancy = {
                "company_name":company_name,
                "position":position,
                "job_description":job_description,
                "key_skills":key_skills
            }

            data.append(vacancy)
    
    return data


def get_api_links(api_param):
    """Синхронная (обычная) функция, получающая список вакансий со страницы. Позволяет запускать с разными параметрами страницы, в основном для изменения страницы

    Args:
        api_param (dict): словарь параметров, в котором в отличие от константы меняется страница запуска

    Returns:
        list: Список вакансий
        int: Количество страниц в запросе
    """
    
    result = get_html(API_URL, api_param)
    pages = result.json().get('pages') # всего страниц в поиске по переданным параметрам
    
    if result.json().get('items') == []:
        return

    link_list = []

    for vacancy in result.json().get('items'):
        link_list.append(vacancy['url'].split('?')[0])
        logger.debug(f"ссылка {vacancy['url'].split('?')[0]} добавлена")
    
    return link_list,pages