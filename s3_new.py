import os
import pandas as pd
import re
import json
import psycopg2
from datetime import datetime

#######
#что хотелось бы добавить:
#1. Уменьшить количество цифр после запятой в 2 раза - НЕТ смысла делать это, там и так чисел мало.
#2. Проредить импульсы для регистрации БД. Записывать каждый 3-й импульс
#3. Так а можно же усредненные данные из файла записывать. + Импульсы средние добавить.


try:
    # пытаемся подключиться к базе данных
    conn = psycopg2.connect("dbname=experiments user=kokos password=jumbo host=92.118.115.115 port=5531")
    print('Соединение установлено...!')
except:
    # в случае сбоя подключения
    print('Can`t establish connection to database...')

# получение объекта курсора
cur = conn.cursor()

#Открываем SQL-скрипта для удаления и создания таблиц. Не будем делать этого.
# with open('create_tables_script_31.12.sql', 'r') as file:
#     script = file.read()
# cur.execute(script)
# conn.commit()

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
def raw_file(element):
    impulses = pd.read_csv(os.path.join(source_folder, value[element]), delimiter=',', header=None)
    impulses.columns = ['Impulse', 'Step', 'Channel'] + [str(i) for i in range(1, 601)]
    impulses[['Impulse', 'Step', 'Channel']] = impulses[['Impulse', 'Step', 'Channel']].astype('category')
    impulses['Sum'] = impulses.iloc[:,8:63].sum(axis=1)
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
                            "impulse_reper": df_impulse[df_impulse['Channel']=='Reper'].iloc[:,3:603].values.flatten().tolist(),
                            "impulse_analyt": df_impulse[df_impulse['Channel']=='Analyt'].iloc[:,3:603].values.flatten().tolist(),
                        },
                        "amplitude_reper": round(list(df_impulse[df_impulse['Channel'] == 'Reper']['Sum'])[0],8),
                        "amplitude_analyt": round(list(df_impulse[df_impulse['Channel'] == 'Analyt']['Sum'])[0],8)
                        }
            output_dict['pulses'].append(pulses_dict)
        final_output[str(f"step_{step_value}")] = output_dict #Вот из этого объекта легко получим все данные.
    return final_output
#Дальше идем по объектам, считываем эти файлы, и записываем в БД. Нужно добавить создание JSON файлы для каждого эксперимента.
for index, value in enumerate(files_dict.values()):
    data_impulse = pd.read_csv(os.path.join(source_folder, value[1]), delimiter='\t', header=None)
    output_filename = os.path.join(source_folder, value[0][5:-4] + ".json")
    print(output_filename)
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
    #Получаем средние значения из файла value[0]
    data_full = pd.read_csv(os.path.join(source_folder, value[0]), delimiter='\t', header=None)
    data_full.columns = ['step_time', 'Step', 'A_Reper', 'A_Analyt', 'Ratio']

    #Дальше нужно записывать шаги и импульсы на них. Информация о времени измерения и шаге находится в элементе value[0] нашего объекта с экспериментом
    #Все же давайте начнем с чтения большого файла value[2]. Читаем объект из value[2]
    raw_object = raw_file(2)
    raw_object_keys = list(raw_object.keys())

    step_data = pd.read_csv(os.path.join(source_folder, value[0]), delimiter='\t', header=None)
    step_data.columns = ['step_time', 'Step', 'A_Reper', 'A_Analyt', 'Ratio']
    exp_id = exp_id
    steps = []
    for idx, step in step_data.iterrows():
        object = data_impulse.iloc[0,idx[1]] #Завтра проверить отсюда
        try:
            data_json = json.loads(object)
            print(data_json["0-Rep;1-Sig"][0])
        except TypeError:
            pass
        step_0 = {"step": idx,
                  "timestamp": step['step_time'],
                  "av_analyt_amp": data_full.loc[idx]['A_Analyt'],
                  "av_reper_amp": data_full.loc[idx]['A_Reper'],
                  "pulses": []
                  }
        start_time = step['step_time']
        step = step['Step'] # Тут по хорошему надо будет сделать И номер И позицию.
        delay_pulses = 200
        insert_query = "INSERT INTO steps (start_time, step, delay_pulses, exp_id) VALUES (%s, %s, %s, %s) RETURNING id"
        data = (start_time, step, delay_pulses, exp_id)
        cur.execute(insert_query, data)
        inserted_record = cur.fetchone()
        step_id = inserted_record[0]
        impulse_object = raw_object[raw_object_keys[idx]]
        tuple_data=[] #Сделать еще одну структуру словарь для записи JSON 
        for i, j in enumerate(impulse_object['pulses']):
            data_dict = {"pulse": i,
                        "pulses": j['pulses'],
                        "amplitude_reper": j['amplitude_reper'],
                        "amplitude_analyt": j['amplitude_analyt']
                  }
            step_0["pulses"].append(data_dict)
            data = (
                step_id,
                json.dumps(j['pulses']),
                j['amplitude_analyt'], 
                j['amplitude_reper'], 
                )
            tuple_data.append(data)
            if i > 5:
                break
#       print(f'Размер tuple data = _{len(tuple_data)}')
        steps.append(step_0)
#       insert_query = "INSERT INTO pulses (step_id, reper_amp, analyt_amp) VALUES (%s, %s, %s)" #Убрал объект с импульсами. Долго записывает. Но опять же для JSON он будет нужен.
#       cur.executemany(insert_query, tuple_data)
#       conn.commit()
        if idx == 5:
            break
    #Данные в JSON файл: 
    my_measurement = {
        'slide': slide,
        'accum_pulses': accum,
        'comment': description,
        'timestamp': value[0][-21:-4],
        'steps': steps}
    with open(output_filename, 'w') as file:
        json.dump(my_measurement, file)
    print('Записан файл...' + output_filename)

#Все это работает, но импульсы записываются очень медленно. Думаю пока обойтись только записью амплитуд.   

    if index > 3:
        break