import os
import pandas as pd
import re
import json
from datetime import datetime
import matplotlib.pyplot as plt

# Указываем папку с файлами. Считываем все *.dat и *.json. Складываем в DataFrame
source_folder = 'd:/Work/2024/data2024/raw'

files = os.listdir(source_folder)
dat_files = [file for file in files if file.endswith(".dat")]
json_files = [file for file in files if file.endswith(".json")]

def extract_date(filename):
    match = re.search(r'\d{2}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}', filename)
    return match.group(0) if match else None

json_dates = set()
for file in json_files:
    date = extract_date(file)
    if date:
        json_dates.add(date)

files_dict = {}
skipped_dates = set()
for file_name in dat_files:
    date = extract_date(file_name)
    if not date:
        continue
    if date in json_dates:
        skipped_dates.add(date)
        continue
    if date in files_dict:
        files_dict[date].append(file_name)
    else:
        files_dict[date] = [file_name]

print(f'Число объектов без json-файлов: {len(files_dict)}')
if skipped_dates:
    print('\nПропущены следующие даты (из-за наличия .json файла):')
    for d in sorted(skipped_dates):
        print(f'  {d}')
else:
    print('\nВсе даты уникальны — .json файлов с такими датами не найдено.')

def raw_file(element, get_every_pulse):
    impulses = pd.read_csv(os.path.join(source_folder, value[element]), delimiter=',', header=None)
    impulses.columns = ['Impulse', 'Step', 'Channel'] + [str(i) for i in range(1, 601)]
    impulses[['Impulse', 'Step', 'Channel']] = impulses[['Impulse', 'Step', 'Channel']].astype('category')
    impulses['Threshold'] = impulses.iloc[:,len(impulses.columns)-100:len(impulses.columns)].mean(axis=1) * (-1)
    impulses.loc[:,[str(i) for i in range(1, 601)]] = impulses.loc[:,[str(i) for i in range(1, 601)]].add(impulses['Threshold'],axis=0)
    impulses['Sum'] = impulses.iloc[:,7:67].sum(axis=1)
    unique_steps = impulses['Step'].unique()
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
                            "pulses": {
                                "impulse_reper": df_impulse[df_impulse['Channel']=='Reper'].iloc[:,3:603].values.flatten().tolist(),
                                "impulse_analyt": df_impulse[df_impulse['Channel']=='Analyt'].iloc[:,3:603].values.flatten().tolist(),
                            },
                            "amplitude_reper": round(list(df_impulse[df_impulse['Channel'] == 'Reper']['Sum'])[0],8),
                            "amplitude_analyt": round(list(df_impulse[df_impulse['Channel'] == 'Analyt']['Sum'])[0],8)
                            }
                output_dict['pulses'].append(pulses_dict)
        final_output[str(f"step_{step_value}")] = output_dict
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

for index, value in enumerate(files_dict.values()):
    print(index)
    data_impulse = pd.read_csv(os.path.join(source_folder, value[1]), delimiter='\t', header=None)
    output_filename = os.path.join(source_folder, s3_filename)
    print(output_filename)
    match = re.search(r'_accum=(\d+)_slide=(\d+)', json.loads(data_impulse.iloc[0,0])["date/time string"])
    if match:
        slide = int(match.group(2))
        accum = int(match.group(1))  
    delay_pts = 5
    pulse_width_pts = 60
    end_offset_pts = 400
    exp_start_time = datetime.strptime(value[0][-21:-4], "%d.%m.%y-%H.%M.%S")
    description = f"Substance_{index+1}"
    substance = f"Substance_{index+1}"
    s3_filename = f"Substance_{index+1}.zip"
    data_full = pd.read_csv(os.path.join(source_folder, value[0]), delimiter='\t', header=None)
    data_full.columns = ['step_time', 'Step', 'A_Analyt', 'A_Reper', 'Ratio']
    raw_object = raw_file(2,2)
    raw_object_keys = list(raw_object.keys())
    steps = []
    for idx, step in data_full.iterrows():
        object = data_impulse.iloc[0,[idx][0]]
        step_time = step['step_time']
        step_time = step_time.replace(',', '.')
        data_json = json.loads(object)
        step_0 = {"step": data_json["Numeric"],
                  "timestamp": step_time,
                  "av_pulses": {
                                'impulse_reper': [round(num,8) for num in data_json["0-Rep;1-Sig"][0] ],
                                'impulse_analyt': [round(num,8) for num in data_json["0-Rep;1-Sig"][1] ]
                                },
                  "av_reper_amp": data_full.loc[idx]['A_Reper'],
                  "av_analyt_amp": data_full.loc[idx]['A_Analyt'],
                  "pulses": []
                  }
        step = step['Step']
        delay_pulses = 200
        impulse_object = raw_object[raw_object_keys[idx]]
        for i, j in enumerate(impulse_object['pulses']):
            pulse_number = int(1+j['pulse']/2)
            data_dict = {"pulse": pulse_number,
                        "pulses": j['pulses'],
                        "amplitude_reper": j['amplitude_reper'],
                        "amplitude_analyt": j['amplitude_analyt']
                  }
            step_0["pulses"].append(data_dict)
        steps.append(step_0)
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
