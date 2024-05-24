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

def find_last_index(text):
    start = text.find('Impulse_')
    end = start + len('Impulse_')
    timestamp = text[len(text)-18:len(text)-4]
    date_parts = timestamp.split("_")
    timestamp = date_parts[0] + ".2023_" + date_parts[1]
    return text[end:len(text)-4], timestamp

try:
    # пытаемся подключиться к базе данных
    conn = psycopg2.connect("dbname=experiments user=kokos password=jumbo host=92.118.115.115 port=5531")
    print('Соединение установлено...!')
except:
    # в случае сбоя подключения
    print('Can`t establish connection to database...')

# получение объекта курсора
cur = conn.cursor()

#Открываем SQL-скрипта для удаления и создания таблиц
with open('create_tables_script_31.12.sql', 'r') as file:
    script = file.read()
cur.execute(script)
conn.commit()


#вот здесь считываются и перезаписываются данные
for index, row in result_2.iterrows():
    data_impulse = pd.read_csv(os.path.join(source_folder, row['Impulse']), delimiter='\t', header=None)
    data_full = pd.read_csv(os.path.join(source_folder, row['FULL']), delimiter='\t', header=None)
    data_full.columns = ['step_time', 'Step', 'A_Reper', 'A_Analyt', 'Ratio']
    data_full_tr = data_full.T
    #здесь сама логика    

    #Сначала заполним данные в таблицу calculations
    #из первого шага/блока получаем accum и slide
    match = re.search(r'_accum=(\d+)_slide=(\d+)', json.loads(data_impulse.iloc[0,0])["date/time string"])
    if match:
        # Извлекаем значения параметров
        slide = int(match.group(2))
        accum = int(match.group(1))  
    # Часть параметров фиксируем на текущий момент
    delay_pts = 5
    pulse_width_pts = 60
    end_offset_pts = 400
    
    # Тут добавляется запись в таблицу calculations. SQL-запрос для проверки наличия записи
    check_query = "SELECT id FROM calculations WHERE slide = %s AND accum_pulses = %s AND delay_pts = %s AND pulse_width_pts = %s AND end_offset_pts = %s"
    # Значение для проверки
    # Выполнение запроса
    cur.execute(check_query, (
        slide,accum,delay_pts,pulse_width_pts, end_offset_pts
        ))
    existing_record = cur.fetchone()
    if existing_record:
    # Если запись уже существует, возвращаем значение поля CalcID
        calc_id = existing_record[0]
    else:
    # Если такой записи нет, то формируем и выполняем запрос INSERT
        insert_query = "INSERT INTO calculations (slide, accum_pulses, delay_pts, pulse_width_pts, end_offset_pts) VALUES (%s, %s, %s, %s, %s) RETURNING id"
        data = (slide, accum, delay_pts, pulse_width_pts, end_offset_pts)
        cur.execute(insert_query, data)
        inserted_record = cur.fetchone()
        calc_id = inserted_record[0]
    # Подтверждение изменений и закрытие соединения
    conn.commit()

    link_str = row['FULL']
    Reper_link = link_str.replace('FULL', 'Reper')
    Analyt_link = link_str.replace('FULL', 'Analyt')
    reper_file = os.path.join(ready_paths[1], Reper_link)
    analyt_file = os.path.join(ready_paths[1], Analyt_link)
    #Пишем в таблицу "experiment"
    start_time = datetime.strptime(find_last_index(row['Impulse'])[1], "%m.%d.%Y_%H.%M.%S")
    description = (row['Impulse'])[13:-19]
    substance = row['Substance']
    calc_id = calc_id
    reper_file_link = reper_file
    analyt_file_link = analyt_file
    insert_query = "INSERT INTO  experiment (start_time, description, substance, calc_id, reper_file_link, analyt_file_link) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id"
    data = (start_time, description, substance, calc_id, reper_file_link, analyt_file_link)
    cur.execute(insert_query, data)
    inserted_record = cur.fetchone()
    exp_id = inserted_record[0]
    conn.commit()
    delay_pulses = 200
    steps = []
    df_reper = pd.DataFrame()
    df_analyt = pd.DataFrame()
    begin_time = time.time()
    tuple_data=[] #Сделать еще одну структуру массив для записи в БД executemany
    for i in zip(data_impulse, data_full_tr):
        object = data_impulse.iloc[0,i[1]]
        try:
            data_json = json.loads(object)
            av_pulses = {'impulse_reper': [round(num,8) for num in data_json["0-Rep;1-Sig"][0] ],
                        'impulse_analyt': [round(num,8) for num in data_json["0-Rep;1-Sig"][1] ]},
            step = {
                    "step": data_json["Numeric"],
                    "timestamp": (data_json["date/time string"])[0:11],
                    "Ratio": float(data_full_tr.loc['Ratio',i[1]].replace(",", ".")),
                    "av_pulses": av_pulses[0],
                    "av_analyt_amp": float(data_full_tr.loc['A_Analyt',i[1]].replace(",", ".")),
                    "av_reper_amp": float(data_full_tr.loc['A_Reper',i[1]].replace(",", ".")),
                    "pulses": []
                    #Данные с сигналами убраны из структуры json. Надо воткнуть smoothed обратно.                 
                    }
            #Записываем данные в таблицу measurements
            data = (
                exp_id,
                (data_json["date/time string"])[0:11].replace(",", "."),
                data_json["Numeric"], # это шаг. 
                delay_pulses,
                json.dumps(av_pulses[0]),
                data_full_tr.loc['A_Analyt',i[1]].replace(",", "."),
                data_full_tr.loc['A_Reper',i[1]].replace(",", ".")
                )
            tuple_data.append(data)
#           df_reper[step['step']] = data_json["0-Rep;1-Sig"][0]
#           df_analyt[step['step']] = data_json["0-Rep;1-Sig"][1]
            steps.append(step)
        except TypeError:
            pass
    insert_query = "INSERT INTO steps (exp_id, start_time, step, delay_pulses, av_pulses, av_analyt_amp, av_reper_amp) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    cur.executemany(insert_query, tuple_data)
    conn.commit()
    end_time = time.time()
    execution_time = end_time - begin_time
    print("Время выполнения операции: {:.5f} секунд".format(execution_time))
    output_filename = os.path.join(output_folder, row['Impulse'][13:-4] + ".json")
    my_measurement = {
        'slide': slide,
        'accum_pulses': accum,
        'comment': find_last_index(row['Impulse'])[0],
        'substance': substance,
        'timestamp': find_last_index(row['Impulse'])[1],
        'Reper_link': Reper_link,
        'Analyt_link': Analyt_link,
        'steps': steps}
    #json_data = json.dumps(my_measurement, separators=(',', ':'))
    # Записываем JSON
    with open(output_filename, 'w') as file:
    #   file.write(json_data)
        json.dump(my_measurement, file)
    print('Записан файл...' + output_filename)
    # if index >= 5:
    #     break
cur.close()
conn.close()