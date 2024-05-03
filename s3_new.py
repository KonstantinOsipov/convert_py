import os
import pandas as pd
import re
import json
import psycopg2
from datetime import datetime

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
    data_impulse = pd.read_csv(os.path.join(source_folder, value[1]), delimiter='\t', header=None)
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

    #Данные для таблицы Experiment
    #Пишем в таблицу "experiment"
    calc_id = calc_id
    start_time = datetime.strptime(value[0][-21:-4], "%d.%m.%y-%H.%M.%S")
    description = value[0][5:-22]
    insert_query = "INSERT INTO  experiment (start_time, description, calc_id) VALUES (%s, %s, %s) RETURNING id"
    data = (start_time, description, calc_id)
    cur.execute(insert_query, data)
    inserted_record = cur.fetchone()
    exp_id = inserted_record[0]
    conn.commit()

    #Дальше нужно записывать шаги и импульсы на них. Информация о времени измерения и шаге находится в элементе value[0] нашего объекта с экспериментом
    #Все же давайте начнем с чтения большого файла value[2].
    impulses = pd.read_csv(os.path.join(source_folder, value[2]), delimiter=',', header=None)
    impulses.columns = ['Impulse', 'Step', 'Channel'] + [str(i) for i in range(1, 601)]
    impulses[['Impulse', 'Step', 'Channel']] = impulses[['Impulse', 'Step', 'Channel']].astype('category')
    print(value[2]) #Вот тут надо что-то делать с этим большим файлом.

    step_data = pd.read_csv(os.path.join(source_folder, value[0]), delimiter='\t', header=None)
    step_data.columns = ['step_time', 'Step', 'A_Reper', 'A_Analyt', 'Ratio']
    exp_id = exp_id
    for idx, step in step_data.iterrows():
        start_time = step['step_time']
        step = step['Step'] # Тут по хорошему надо будет сделать И номер И позицию.
        delay_pulses = 200
        insert_query = "INSERT INTO steps (start_time, step, delay_pulses, exp_id) VALUES (%s, %s, %s, %s) RETURNING id"
        data = (start_time, step, delay_pulses, exp_id)
        cur.execute(insert_query, data)
        inserted_record = cur.fetchone()
        step_id = inserted_record[0]
        if idx == 5:
            break

        #А теперь в каждый шаг нужно добавлять импульсы из элемента value[2]. (В этом же цикле). Надо бы этот файл отдельно пообрабатывать в pandas. 
        #Тут понятно. проще наверное сначала этот большой объект прочитать а потом INSERT-ить данные в таблицу Steps, и pulses .. 
    
    impulses = pd.read_csv(os.path.join(source_folder, value[2]), delimiter=',', header=None)
    impulses.columns = ['Impulse', 'Step', 'Channel'] + [str(i) for i in range(1, 601)]
    impulses[['Impulse', 'Step', 'Channel']] = impulses[['Impulse', 'Step', 'Channel']].astype('category')
    impulses['Sum'] = impulses.iloc[:,8:63].sum(axis=1)
    print(impulses.head(3))
    unique_steps = impulses['Step'].unique()
    # Initialize the final output dictionary
    final_output = {}
    for step_value in unique_steps:
        df_step = impulses[impulses['Step'] == step_value]
        output_dict = {
            "step": step_value,
            "pulses": []
        }
        for impulse_value in df_step['Impulse'].unique():
            df_impulse = df_step[df_step['Impulse'] == impulse_value]
            pulses_dict={"pulse": impulse_value,
                        "pulses": {
                            "impulse_reper": df_impulse[df_impulse['Channel']=='Reper'].iloc[:,4:604].values.flatten().tolist(),
                            "impulse_analyt": df_impulse[df_impulse['Channel']=='Analyt'].iloc[:,4:604].values.flatten().tolist(),
                        },
                        "amplitude_reper": round(list(df_impulse[df_impulse['Channel'] == 'Reper']['Sum'])[0],4),
                        "amplitude_analyt": round(list(df_impulse[df_impulse['Channel'] == 'Analyt']['Sum'])[0],4)
                        }
            output_dict['pulses'].append(pulses_dict)
        final_output[str(f"step_{step_value}")] = output_dict #Вот из этого объекта легко получим все данные.

    if index >= 1:
        break