import os
import pandas as pd
#Через S3 не получается. Все равно нужно копировать файлы чтоыб их распаковать
#Указываем папку с файлами. Считываем все *.dat . И складываем в DataFrame
source_folder = 'd:/Work/2024/data2024/raw'
files = os.listdir(source_folder)
files = [file for file in files if file.endswith(".dat")]
print(len(files))
#Теперь имена файлов сведем в таблицу для дальнейшего считывания и распределения по объектам в БД
# for file in files:
#     print(file[-21:-4])
files_dict = {}
for index, file_name in enumerate(files):
    date = file_name[-21:-4]  # Получаем дату из имени файла
    if date in files_dict:
        files_dict[date].append(file_name)
    else:
        files_dict[date] = [file_name]
print(f'Число объектов, {len(files_dict)}'
      )
#Дальше идем по объектам, считываем эти файлы, и записываем в БД
for index, value in enumerate(files_dict.values()):
    data_impulse = pd.read_csv(os.path.join(source_folder, value[0]), delimiter='\t', header=None)
    print(data_impulse)
    if index == 2: 
        break

