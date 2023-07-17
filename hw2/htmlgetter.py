from constants import *
import logging
import logging.config
import requests

#Настраиваем логгер
logging.config.fileConfig(fname=PATH_LOG, disable_existing_loggers=False)
logger = logging.getLogger('htmlLogger')


def get_html(link, url_params = ""):
    """Получает страницу с указанного url в формате для дальнейшей обработки

    Args:
        link (str): ссылка, которую нужно вызывать
        url_params (dict): параметры url вызываемой страницы, могут быть опущены. Defaults to "".

    Args:
        url (str): Передаваемый url в виде строки

    Returns:
        object: контент страницы в формате функции requests.get, нужно обработать в нужном виде
    """
    try:
        logger.debug(f'Загружаем информацию со следующей страницы: {link}')
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
        logger.debug(f'Загрузка страницы: {link} успешно завершена')
    except Exception as e:
            logger.error(f"При загрузке страницы {link} возникла ошибка. Исключение: {e}")
            return 
    
    return data