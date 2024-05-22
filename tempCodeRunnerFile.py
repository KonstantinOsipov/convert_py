#Импорты
import pandas as pd
import os
import re
import json
import shutil
import psycopg2
import time
from datetime import datetime
#Внимание! 01.05.2024 Поменял структуру БД. Эти файлы должны записываться в новую структуру. И по файлам JSON получается
#Открываем папку. Считываем все .dat. И складываем в DataFrame
source_folder = 'd:/Work/2023/TimeWeb_data/'
files = os.listdir(source_folder)
files = [file for file in files if file.endswith(".dat")]
print(len(files))
#Сформируем пути для записи выходных данных
path_array = source_folder.split('/')
path_array = path_array[0:len(path_array)-2]
path_array.append('Output')
smooth_path_array = path_array + ['Smoothed']
raw_path_array = path_array + ['Raw']
paths = [path_array, smooth_path_array, raw_path_array]
separator = '/'  # Разделительный символ
ready_paths=[]
for paths in paths: 
    folder = separator.join(paths)
    folder += "/"
    try:
        os.mkdir(folder)
    except FileExistsError:
        pass
    ready_paths.append(folder)
output_folder = ready_paths[0]

impulse_df = pd.DataFrame(columns=['FileName','datetime'])
full_df = pd.DataFrame(columns=['FileName','datetime'])
for file in files:
    if file.startswith('Impulse_'):
        impulse_df = impulse_df._append({'FileName': file, 'datetime': file[len(file)-18:len(file)-4] } ,ignore_index=True)
    else:
        full_df = full_df._append({'FileName': file, 'datetime': file[len(file)-18:len(file)-4] } ,ignore_index=True)
result = pd.merge(impulse_df, full_df, on='datetime')

def extract_substance_name(row):
    start_index = row.find('FULL_') + len('FULL_')
    end_index = row.find('_', start_index)
    if start_index != -1 and end_index != -1:
        return row[start_index:end_index]
    else:
        return "Название не найдено"

result_2 = pd.DataFrame({
    'Impulse': result['FileName_x'],
    'FULL': result['FileName_y']
})

result_2['Substance'] = result_2['FULL'].apply(extract_substance_name)
for i in result_2['Substance']:
    print(i)
