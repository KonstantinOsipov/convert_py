import pandas as pd
import os
import json

source_folder = 'd:/Work/2024/data2024/raw'
file = 'raw_Вода_5_04.03.24-15.37.50.dat'
impulses = pd.read_csv(os.path.join(source_folder, file), delimiter=',', header=None)
impulses.columns = ['Impulse', 'Step', 'Channel'] + [str(i) for i in range(1, 601)]
impulses[['Impulse', 'Step', 'Channel']] = impulses[['Impulse', 'Step', 'Channel']].astype('category')

impulses['Sum'] = impulses.iloc[:,8:63].sum(axis=1)

print(
     impulses.head()) #Вот тут надо что-то делать с этим большим файлом.

data = {
    'step': [0, 0, 1, 1, 2, 2],  # Пример значений шагов, повторяющихся три раза
    'amplitude': [100, 1.6, 150, 2.2, 200, 2.2],  # Пример значений амплитуды
    'channel': ['Reper', 'Analyt', 'Reper', 'Analyt', 'Reper', 'Analyt'],
    'impulse_array_element_1': [1.5, 1.6, 2.1, 2.2, 2.0, 2.2],
    'impulse_array_element_2': [1.5, 1.6, 2.1, 2.2, 2.0, 2.2],
    'impulse_array_element_3': [1.5, 1.6, 2.1, 2.2, 2.0, 2.2],
    'impulse_array_element_n': [1.5, 1.6, 2.1, 2.2, 2.0, 2.2]
}

df = pd.DataFrame(data)

# Создадим словарь для объединения данных по каждому шагу и каналу
steps_dict = {}

# Итерируем по уникальным значениям шагов и создаем словарь для каждого шага
for step in df['step'].unique():
    step_data = df[df['step'] == step]
    step_dict = {
        "step": step,
        "pulses": {
            "impulse_reper": [],
            "impulse_analyt": []
        },
        "amplitude_reper": step_data.loc[step_data['channel'] == 'Reper', 'amplitude'],
        "amplitude_analyt": step_data.loc[step_data['channel'] == 'Analyt', 'amplitude']
    }
    
    for index, row in step_data.iterrows():
        if row['channel'] == 'Reper':
            impulse_reper = [row[col] for col in row.index if col.startswith('impulse_array_element_')]
            step_dict["pulses"]["impulse_reper"].append(impulse_reper)
        elif row['channel'] == 'Analyt':
            impulse_analyt = [row[col] for col in row.index if col.startswith('impulse_array_element_')]
            step_dict["pulses"]["impulse_analyt"].append(impulse_analyt)

    steps_dict[step] = step_dict
print(steps_dict)

json_data = json.dumps(steps_dict, indent=4)
print(json_data)
