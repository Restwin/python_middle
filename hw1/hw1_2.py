import sqlite3
from zipfile import ZipFile
import json

path = "D:\\Py\\01pml\\dz1\\egrul.json.zip"

print('Создаем Базу данных и таблицу telecom_companies')
conn = sqlite3.connect('hw.db')
cur = conn.cursor()

cur.execute("""CREATE TABLE IF NOT EXISTS telecom_companies(
   name TEXT,
   full_name TEXT,
   inn TEXT,
   kpp TEXT,
   okved TEXT
   );
""")
conn.commit()

print('База данных и таблица созданы, открываем файл egrul.json.zip (может занять какое-то время)')
with ZipFile(path) as myzip:
    
    for file_okved in myzip.namelist():
        with myzip.open(file_okved) as okved_file:
            print('Смотрим файл ', okved_file)
            data = json.load(okved_file)

            for string in data:
                if 'СвОКВЭД' in string['data']:
                    if 'СвОКВЭДОсн' in string['data']['СвОКВЭД']:
                        okved = string['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД']
                        if okved[:2] == '61':
                            instring = (string['name'], string['full_name'], string['inn'], string['kpp'], okved)
                            cur.execute("INSERT INTO telecom_companies VALUES(?, ?, ?, ?, ?);", instring)
                            conn.commit()

cur.execute("SELECT count(*) FROM telecom_companies;")
one_result = cur.fetchone()
print('Данные успешно записаны. Количество строк: ', one_result[0])

#Можно сразу зачистить данные:
delete_table = input('Если таблицу нужно удалить, введите 1')
if delete_table == '1':
    cur.execute("DROP TABLE IF EXISTS telecom_companies;")
    conn.commit()
    print ('Таблица удалена')
else:
    print ('Таблица НЕ удалялась')

conn.close()