import sqlite3
from zipfile import ZipFile
import json

#поменять расположение файла, при необходимости
path = "D:\\Py\\01pml\\dz1\\okved_2.json.zip"

print('Создаем Базу данных и таблицу okved')
conn = sqlite3.connect('hw.db')
cur = conn.cursor()

cur.execute("""CREATE TABLE IF NOT EXISTS okved(
   code TEXT,
   parent_code TEXT,
   section TEXT,
   name TEXT,
   comment TEXT
   );
""")
conn.commit()

print('База данных и таблица созданы, открываем файл okved_2.json.zip (может занять какое-то время)')
with ZipFile(path) as myzip:
    file_okved = myzip.namelist()[0]
    with myzip.open(file_okved) as myfile:
        data = json.load(myfile)

print('Пишем содержимое файла в таблицу okved')
for string in data:
    instring = (string['code'], string['parent_code'], string['section'], string['name'], string['comment'])
    cur.execute("INSERT INTO okved VALUES(?, ?, ?, ?, ?);", instring)
    conn.commit()
 
cur.execute("SELECT count(*) FROM okved;")
one_result = cur.fetchone()
print('Данные успешно записаны. Количество строк: ', one_result[0])

#Можно сразу зачистить данные:
delete_table = input('Если таблицу нужно удалить, введите 1')
if delete_table == '1':
    cur.execute("DROP TABLE IF EXISTS okved;")
    conn.commit()
    print ('Таблица удалена')
else:
    print ('Таблица НЕ удалялась')

conn.close()