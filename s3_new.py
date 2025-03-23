import os
import pandas as pd
import re
import json
import psycopg2
import time
from datetime import datetime
import matplotlib.pyplot as plt

#######
#что хотелось бы добавить:
#1. Уменьшить количество цифр после запятой в 2 раза - НЕТ смысла делать это, там и так чисел мало.
#2. Проредить импульсы для регистрации БД. Записывать каждый 3-й импульс. Вместо 600 оставить 200.
#3. Так а можно же усредненные данные из файла записывать. + Импульсы средние добавить. DONE.
#4. Удялять бы из таблиц experiment и step все записи, кроме 2023 года.
#5. 03.09.2024 Пока непонятно как дальше быть. Не охота пихать все импульсы в БД. Пусть лежат в S3 хранилище, и будет ссылка на них.
#6. График не будем делать. Так, убираем из записи в БД ВСЕ импульсы. Остаются только их параметры.
#7. Хочу вернуть запись усредненных импульсов, и самих импульсов в файлы json.


try:
    # пытаемся подключиться к базе данных
    conn = psycopg2.connect("dbname=experiments user=kokos password=jumbo host=127.0.0.1 port=5531")
    print('Соединение установлено...!')
except:
    # в случае сбоя подключения
    print('Can`t establish connection to database...')

# получение объекта курсора
cur = conn.cursor()

#Открываем SQL-скрипта для удаления и создания таблиц. Ничего не удаляем.
# with open('create_tables_script_31.12.sql', 'r') as file:
#     script = file.read()
# cur.execute(script)
# conn.commit()

#Через S3 не получается. Все равно нужно копировать файлы чтоыб их распаковать
#Указываем папку с файлами. Считываем все *.dat . И складываем в DataFrame

source_folder = 'd:/Work/2024/data2024/raw'

# Получаем список всех файлов в папке
files = os.listdir(source_folder)

# Фильтруем по расширению
dat_files = [file for file in files if file.endswith(".dat")]
json_files = [file for file in files if file.endswith(".json")]

# Функция извлечения даты из имени файла
def extract_date(filename):
    match = re.search(r'\d{2}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}', filename)
    return match.group(0) if match else None

# Извлекаем даты из .json файлов
json_dates = set()
for file in json_files:
    date = extract_date(file)
    if date:
        json_dates.add(date)

# Формируем словарь .dat файлов, исключая те, у которых уже есть соответствующий .json
files_dict = {}
skipped_dates = set()

for file_name in dat_files:
    date = extract_date(file_name)
    if not date:
        continue  # Пропускаем, если дату не удалось извлечь
    if date in json_dates:
        skipped_dates.add(date)
        continue
    if date in files_dict:
        files_dict[date].append(file_name)
    else:
        files_dict[date] = [file_name]

# Вывод результатов
print(f'Число объектов без json-файлов: {len(files_dict)}')

# Лог пропущенных дат
if skipped_dates:
    print('\nПропущены следующие даты (из-за наличия .json файла):')
    for d in sorted(skipped_dates):
        print(f'  {d}')
else:
    print('\nВсе даты уникальны — .json файлов с такими датами не найдено.')

def raw_file(element, get_every_pulse): #Параметр get_every_pulse нужен для того, чтобы прореживать импульсы. В данном случае мы берем каждый второй импульс
    impulses = pd.read_csv(os.path.join(source_folder, value[element]), delimiter=',', header=None)
    impulses.columns = ['Impulse', 'Step', 'Channel'] + [str(i) for i in range(1, 601)]
    impulses[['Impulse', 'Step', 'Channel']] = impulses[['Impulse', 'Step', 'Channel']].astype('category')
    impulses['Threshold'] = impulses.iloc[:,len(impulses.columns)-100:len(impulses.columns)].mean(axis=1) * (-1)
    impulses.loc[:,[str(i) for i in range(1, 601)]] = impulses.loc[:,[str(i) for i in range(1, 601)]].add(impulses['Threshold'],axis=0)
    impulses['Sum'] = impulses.iloc[:,7:67].sum(axis=1)
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
            if impulse_value % get_every_pulse == 0:
                pulses_dict={"pulse": impulse_value,
                            "pulses": { #Здесь можно закоментить считывание объекта. Объект с импульсами не нужен, не будем его считывать. Это ускорит дело.
                                "impulse_reper": df_impulse[df_impulse['Channel']=='Reper'].iloc[:,3:603].values.flatten().tolist(),
                                "impulse_analyt": df_impulse[df_impulse['Channel']=='Analyt'].iloc[:,3:603].values.flatten().tolist(),
                            },
                            "amplitude_reper": round(list(df_impulse[df_impulse['Channel'] == 'Reper']['Sum'])[0],8),
                            "amplitude_analyt": round(list(df_impulse[df_impulse['Channel'] == 'Analyt']['Sum'])[0],8)
                            }
                output_dict['pulses'].append(pulses_dict)
        final_output[str(f"step_{step_value}")] = output_dict #Вот из этого объекта легко получим все данные.
    return final_output
def extract_substance_name(row):
    start_index = row.find('data_') + len('data_')
    end_index = row.find('_', start_index)
    comment = row[start_index:end_index]
    s3_filename = row[start_index:len(row)-4]+'.zip'
    if start_index != -1 and end_index != -1:
        return comment, s3_filename
    else:
        return "Название не найдено"
#Дальше идем по объектам, считываем эти файлы, и записываем в БД. Нужно добавить создание JSON файлы для каждого эксперимента.
for index, value in enumerate(files_dict.values()):
    print(index)
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
    exp_start_time = datetime.strptime(value[0][-21:-4], "%d.%m.%y-%H.%M.%S")
    description = value[0][5:-22]
    substance = extract_substance_name(value[0])[0]
    s3_filename = extract_substance_name(value[0])[1]
    insert_query = "INSERT INTO experiment (start_time, description, substance, calc_id, reper_file_link) VALUES (%s, %s, %s, %s, %s) RETURNING id"
    data = (exp_start_time, description, substance, calc_id, s3_filename)
    cur.execute(insert_query, data)
    inserted_record = cur.fetchone()
    exp_id = inserted_record[0]
    conn.commit()
    #Получаем средние значения из файла value[0] - этой файл data_*.* где находятся данные по амплиутуде!
    data_full = pd.read_csv(os.path.join(source_folder, value[0]), delimiter='\t', header=None)
    data_full.columns = ['step_time', 'Step', 'A_Analyt', 'A_Reper', 'Ratio']
    #Дальше нужно записывать шаги и импульсы на них. Информация о времени измерения и шаге находится в элементе value[0] нашего объекта с экспериментом
    #Все же давайте начнем с чтения большого файла value[2]. Читаем объект из value[2]
    raw_object = raw_file(2,2)
    raw_object_keys = list(raw_object.keys())
    exp_id = exp_id
    steps = []
    for idx, step in data_full.iterrows():
        object = data_impulse.iloc[0,[idx][0]]
        step_time = step['step_time']
        step_time = step_time.replace(',', '.')
        data_json = json.loads(object)
        step_0 = {"step": data_json["Numeric"],
                  "timestamp": step_time,
                  "av_pulses": { #Если нужно, можно закоментить запись УСРЕДНЕННЫХ импульсов.
                                'impulse_reper': [round(num,8) for num in data_json["0-Rep;1-Sig"][0] ],
                                'impulse_analyt': [round(num,8) for num in data_json["0-Rep;1-Sig"][1] ]
                                },
                  "av_reper_amp": data_full.loc[idx]['A_Reper'],
                  "av_analyt_amp": data_full.loc[idx]['A_Analyt'],
                  "pulses": [] #массив с импульсами на каждом шаге еще не заполнен
                  }
        step = step['Step'] # Тут по хорошему надо будет сделать И номер И позицию.
        delay_pulses = 200
        insert_query = "INSERT INTO steps (start_time, step, delay_pulses, exp_id, av_analyt_amp, av_reper_amp) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id"
        averaged = (step_time, step, delay_pulses, exp_id,
                data_full.loc[idx]['A_Analyt'],
                data_full.loc[idx]['A_Reper']
                )
        cur.execute(insert_query, averaged)
        inserted_record = cur.fetchone()
        step_id = inserted_record[0]
        conn.commit()
        # print(f'Записали шаг {data_json["Numeric"]}, эксперимента {value[0]}')
        impulse_object = raw_object[raw_object_keys[idx]]
        tuple_data=[] #Сделать еще одну структуру словарь для записи JSON
        for i, j in enumerate(impulse_object['pulses']):
            pulse_number = int(1+j['pulse']/2)
            data_dict = {"pulse": pulse_number,
                        "pulses": j['pulses'], #7. Можно закоментировать объект с имульсами для записи в файл. Вот были импульсы для записи, я их убрал
                        "amplitude_reper": j['amplitude_reper'],
                        "amplitude_analyt": j['amplitude_analyt']
                  }
            step_0["pulses"].append(data_dict)
            data = (
                step_id,
                pulse_number,
#               [], # #Тут были массивы json.dumps(j['pulses']), но очень долго записывается в базу
                j['amplitude_analyt'], 
                j['amplitude_reper'], 
                )
            tuple_data.append(data)
            # if i > 30:
            #     break
        steps.append(step_0) # Есть ошибка, нужно номер импульса добавить для записи в БД. таблица pulses.
        insert_query = "INSERT INTO pulses (step_id, pulse_number, analyt_amp, reper_amp) VALUES (%s, %s, %s, %s)" #Убрал объект с импульсами. Долго записывает. Но опять же для JSON он будет нужен.
        cur.executemany(insert_query, tuple_data)
        conn.commit()
        # if idx == 3:
        #     break
    #Данные в JSON файл: 
    my_measurement = {
        'dataset': 'Dataset1',
        'slide': slide,
        'accum_pulses': accum,
        'comment': description,
        'substance': substance,
        'timestamp': exp_start_time.isoformat(),
        'file_link': s3_filename,
        'steps': steps}
    with open(output_filename, 'w') as file:
        json.dump(my_measurement, file)
    print('Записан файл...' + output_filename)

#Все это работает, но импульсы записываются очень медленно. Думаю пока обойтись только записью амплитуд.   

    # if index > 5:
    #     break
cur.close()
conn.close()