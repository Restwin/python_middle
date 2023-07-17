from constants import *
from api import *
from pars import *
from db import *
import logging
import logging.config
from time import sleep

import json

#Настраиваем логгер
logging.config.fileConfig(fname=PATH_LOG, disable_existing_loggers=False)
logger = logging.getLogger('mainLogger')

def pars_mode():
    """Запуск режима парсинга. Параметры парсинга установлены в constants.py
    """
    logger.info('Запуск поиска вакансий через парсинг')
    data = []
    for link in get_pars_links():
        sleep(TIME_SLEEP)
        logger.debug(f'Берем следующую ссылку и парсим: {link}')
        result = get_pars_vacancies(link)
        if result is not None:
            data.append(result)
        #Проверить количество, если достаточно - закончить
        if len(data) == PARS_LIMIT_VACNCY:
            logging.info(f'Отобрано {PARS_LIMIT_VACNCY} вакансий')
            break

    #Проверить количество
    if len(data) < PARS_LIMIT_VACNCY:
        logger.warning(f"Всего вакансий = {len(data)}, что меньше {PARS_LIMIT_VACNCY} штук. Либо сайт больше не отдаёт, либо столько нет вакансий в городе и для получения больше вакансий нужно увеличить количество городов в настройках (и изменить логику в коде)")

    write_db(data, PARS_DB_TABLE)
    logger.info(f'Работа режима через парсинг завершена')

def api_mode():  
    """Запуск режима api. Параметры парсинга установлены в constants.py
    """
    logger.info('Запуск поиска вакансий через api')
    data = []
    page = 0

    while len(data) < API_LIMIT_VACNCY:
        logger.debug(f'Получаем {page+1} партию ссылок')     
        links,pages = get_api_links(http_params)
        
        # проверяем, не пустой ли последний вызов (значит больше их нет и проверка на страницы почему-то не сработала, или словили капчу)
        if links is None:
            break
        
        if len(links)+len(data) > API_LIMIT_VACNCY*1.4:
            logger.debug(f'Оставляем только необходимое количество ссылок + 40% запаса на отсев')
            links = links[:int(API_LIMIT_VACNCY*1.4-len(data))]

        logger.debug(f'Отправляем партию ссылок в асинхронное получение')
        data_temp = asyncio.run(get_api_vacncies(links))

        logger.debug(f'Проверяем партию результатов на наличие скилов и отсеиваем')
        for item in data_temp:
            if item['key_skills'] == []:
                logger.debug(f'Вакансия {item["name"]} без ключевых навыков')
                continue
            data.append(item)

            if len(data) == API_LIMIT_VACNCY:
                break
        if len(data) == API_LIMIT_VACNCY:
                break
        # Повышаем номер страницы
        page=+1
        if page == pages:
            #больше нет страниц
            break
        # Иначе идем на следующий круг
        # Вообще, менять константу нехорошо, но я не придумал, как лучше инкрементировать страницу, сохраняя одно значение параметров для всех
        http_params['page'] = page

        logger.debug(f'Нужна ещё одна страница, засыпаем на {TIME_SLEEP} сек для уменьшения риска получения капчи')
        sleep(TIME_SLEEP) # Чуть замираем, чтобы не словить каппчу

    #Проверить количество
    if len(data) < API_LIMIT_VACNCY:
        logger.warning(f"Всего вакансий = {len(data)}, что меньше {API_LIMIT_VACNCY} штук. Для получения больше вакансий нужно увеличить количество городов в настройках")
    
    write_db(data, API_DB_TABLE)
    
    logger.info(f'Работа режима через апи завершена')


def main():
    """Основная функция запуска приложения
    """
    try:
        logger.info('Запуск приложения')
        # режимы запуска
        mode = input(""" \n
                        Программа выполняет парсинг вакансий с сайта hh.ru по заранее заданному списку слов\n
                        В каком режиме запустить?\n
                        (1) - Оба варианта: парсинг и api\n
                        (2) - Только парсинг\n
                        (3) - Только api\n
                        (4) - Удаление содержимого БД (чтобы перепроверить на чистую)\n
                        (-) - Любой символ для выхода без выполнения
                    """)
        logger.info('Получен ответ от пользователя {mode}')

        try:
            mode = int(mode)
        except:
            print('Режим не выбран. Программа завершена.')
            return            
        if mode == 1:
            logger.info('Запуск режима объединенного поиска вакансий (парсинга и через api)')
            pars_mode()
            api_mode()

        elif mode ==2:
            logger.info('Запуск режима поиска вакансий через api')
            pars_mode()

        elif mode ==3:
            logger.info('Запуск режима поиска вакансий через парсинг')
            api_mode()

        elif mode ==4:
            logger.info('Запуск режима удаления содержимого бд')
            del_tables()

        else:
            print('Режим не выбран. Программа завершена.')
            return
        
    except Exception as e:
        logger.critical(f"При запуске приложения возникла неизвестная ошибка. Исключение: {e}")
        raise 
    

if __name__ == '__main__':
    main()

