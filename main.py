#Импорты
import pandas as pd
import os
import re
import json
import shutil
import psycopg2
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
result_2 = pd.DataFrame({
    'Impulse': result['FileName_x'],
    'FULL': result['FileName_y']
})

def find_last_index(text):
    start = text.find('Impulse_')
    end = start + len('Impulse_')
    timestamp = text[len(text)-18:len(text)-4]
    date_parts = timestamp.split("_")
    timestamp = date_parts[0] + ".2023_" + date_parts[1]
    return text[end:len(text)-4], timestamp

try:
    # пытаемся подключиться к базе данных
    conn = psycopg2.connect("dbname=experimentdb user=kokos password=Mov@lis28 host=92.118.115.115")
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
    calc_id = calc_id
    reper_file_link = reper_file
    analyt_file_link = analyt_file
    insert_query = "INSERT INTO  experiment (start_time, description, calc_id, reper_file_link, analyt_file_link) VALUES (%s, %s, %s, %s, %s) RETURNING id"
    data = (start_time, description, calc_id, reper_file_link, analyt_file_link)
    cur.execute(insert_query, data)
    inserted_record = cur.fetchone()
    exp_id = inserted_record[0]
    conn.commit()
    
    steps = []
    df_reper = pd.DataFrame()
    df_analyt = pd.DataFrame()
    for i in zip(data_impulse, data_full_tr):
        object = data_impulse.iloc[0,i[1]]
        try:
            data_json = json.loads(object)
            step = {
                    "step": data_json["Numeric"],
                    "timestamp": (data_json["date/time string"])[0:11],
                    "A_Reper": data_full_tr.loc['A_Reper',i[1]],
                    "A_Analyt": data_full_tr.loc['A_Analyt',i[1]],
                    "Ratio": data_full_tr.loc['Ratio',i[1]],
                    "Smoothed": (data_json["0-Rep;1-Sig"])
                    #Данные с сигналами убраны из структуры json                  
                    }
            #Записываем данные в таблицу measurements
            insert_query = "INSERT INTO measurements (exp_id, start_time, step, reper_energy, analyt_energy, rep_analyt_ratio) VALUES (%s, %s, %s, %s, %s, %s)"
            data = (
                exp_id,
                (data_json["date/time string"])[0:11].replace(",", "."),
                data_json["Numeric"],
                data_full_tr.loc['A_Reper',i[1]].replace(",", "."),
                data_full_tr.loc['A_Analyt',i[1]].replace(",", "."),
                data_full_tr.loc['Ratio',i[1]].replace(",", ".")
            )
            cur.execute(insert_query, data)
            conn.commit()
            df_reper[step['step']] = data_json["0-Rep;1-Sig"][0]
            df_analyt[step['step']] = data_json["0-Rep;1-Sig"][1]
            steps.append(step)
        except TypeError:
            pass
    output_filename = os.path.join(output_folder, row['Impulse'][13:-4] + ".json")
    

    my_measurement = {
        'slide': slide,
        'accum_pulses': accum,
        'comment': find_last_index(row['Impulse'])[0],
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
    df_reper.to_csv(reper_file,index=False)
    df_analyt.to_csv(analyt_file,index=False)

    if index == 5:
        break
cur.close()
conn.close()