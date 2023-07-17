from constants import *
from htmlgetter import get_html
import logging
import logging.config
from bs4 import BeautifulSoup
import json

#Настраиваем логгер
logging.config.fileConfig(fname=PATH_LOG, disable_existing_loggers=False)
logger = logging.getLogger('parsLogger')

#Получаем ссылки вакансий с помощью генератора yield
def get_pars_links():
    """Функция поиска вакансий на странице поиска hh.ru \n
       Функция без параметров, поисковая строка устанавливается в
       файле constants.py в переменную url_params
    """
    #закомментировано за ненадобностью, но хороший вариант для нескольких территорий
    """ код среализацией нескольких городов, убран за несопоставимостью с задачей
    #Выбраны города Москва, Санкт-Петербург, в параметрах area
    #Для увеличения, нужно добавить в файле constants.py в переменную area_list новое значение
    area = ""
    for i in range(len(area_list)):
        area += f"&area={area_list[i]}"
    
    url = f"https://hh.ru/search/vacancy?no_magic=true&L_save_area=true&text={url_params['text']}&search_field={url_params['search_field']}&excluded_text={area}&items_on_page={url_params['per_page']}"
    """
    url = PARS_URL_SEARCH
    params = http_params
    content = get_html(url, params).content

    #поиск ссылок
    try:
        soup = BeautifulSoup(content, "lxml")

        # основная страница (20 штук)
        for vacancy in soup.find_all("a", attrs = {"class":"serp-item__title"}):
            yield f"{vacancy.attrs['href'].split('?')[0]}"

        # подзагрузка template (остальное)
        for vacancy in json.loads(soup.find('template', attrs={'id': 'HH-Lux-InitialState'}).text)['vacancySearchResult']['vacancies']:
            yield vacancy.get('links').get('desktop')

    except Exception as e:
        logger.error(f"При получении ссылок со страницы возникла ошибка. Исключение: {e}")
        raise

def get_pars_vacancies(link):
    """Функция сбора информации о каждой вакансии \n
       На вход подаётся только одна переменная с url вакансии

    Args:
        link (string): ссылка на вакансию в формате "https://hh.ru/vacancy/00000000"
        
    Returns:
        dict: словарь вакансии с полями в формате:
        {
        "company_name":company_name,
        "position":position,
        "job_description":job_description,
        "key_skills":key_skills
        }
    """
    #поиск данных    
    try: 
        content = get_html(link).content

        logger.debug(f'Начало парсинга содержимого страницы: {link}')
        soup = BeautifulSoup(content, "lxml")
        company_name = soup.find(attrs = {"class":"vacancy-company-details"}).text.replace("\xa0", " ")
        position = soup.find(attrs = {"class":"vacancy-title"}).text.replace("\xa0", " ")
        job_description = soup.find(attrs = {"class":"vacancy-description"}).text.replace("\xa0", " ")
        key_skills = [tag.text.replace("\xa0", " ") for tag in soup.find(attrs = {"class":"bloko-tag-list"}).find_all(attrs = {"class":"bloko-tag__section_text"})]
        logger.debug(f'Парсинг содержимого страницы: {link} завершен')
    except Exception as e:
        logger.error(f"При парсинге со страницы {link} отсутствовало поле (скорее всего ключевые навыки, при ошибке find_all). Исключение: {e}")
        return 

    #сбор объекта
    vacancy = {
        "company_name":company_name,
        "position":position,
        "job_description":job_description,
        "key_skills":key_skills
    }
    return vacancy