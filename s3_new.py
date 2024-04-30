import os
#Через S3 не получается. Все равно нужно копировать файлы чтоыб их распаковать
#Указываем папку с файлами. Считываем все *.dat . И складываем в DataFrame
source_folder = 'd:/Work/2024/data2024/raw'
files = os.listdir(source_folder)
files = [file for file in files if file.endswith(".dat")]
print(len(files))
#Теперь имена файлов сведем в таблицу для дальнейшего считывания и распределения по объектам в БД
for file in files:
    print(file[-21:-4])
files_dict = {}
for index, file_name in enumerate(files):
    date = file_name[-21:-4]  # Получаем дату из имени файла
    rem = index // 3
    name = 'Exp_'+ str(rem)
    if date in files_dict:
        files_dict[date].append(file_name)
    else:
        files_dict[date] = [file_name]
print(files_dict)
