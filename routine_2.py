import random
import pandas as pd
import os
import json

source_folder = 'd:/Work/2024/data2024/raw'
file = 'raw_Вода_5_04.03.24-15.37.50.dat'
impulses = pd.read_csv(os.path.join(source_folder, file), delimiter=',', header=None)
impulses.columns = ['Impulse', 'Step', 'Channel'] + [str(i) for i in range(1, 601)]
impulses[['Impulse', 'Step', 'Channel']] = impulses[['Impulse', 'Step', 'Channel']].astype('category')

impulses['Sum'] = impulses.iloc[:,8:63].sum(axis=1)

# print(
#      impulses.head()) #Вот тут надо что-то делать с этим большим файлом.

names = ["Amplitude", "el_1", "el_2", "el_3", "el_4"]
random_lists = [[round(random.uniform(1.0, 100.0),2) for _ in range(24)] for _ in range(len(names))]
result = dict(zip(names, random_lists))

data = {
    'Impulse': [item for i in range(6) for item in [i]*2]*2,
    'Channel': ['Reper', 'Analyt']*12,
    'step': [0]*12 + [1]*12
}
data.update(result)

df = pd.DataFrame(data)
print(df)

# Get unique step values from the DataFrame
unique_steps = df['step'].unique()

# Initialize the final output dictionary
final_output = {}

# Iterate over each unique step value
for step_value in unique_steps:
    df_step = df[df['step'] == step_value]

    # Initialize the dictionary structure for the current step
    output_dict = {
        "step": step_value,
        "pulses": []
    }

    # Iterate over each row in the filtered DataFrame for the current step
    for index, row in df_step.iterrows():
        pulse_dict = {
            "pulse": row['Impulse'],
            "pulses": {
                "impulse_reper": [],
                "impulse_analyt": []
            },
            "amplitude_reper": row['amplitude'] if row['Channel'] == 'Reper' else None,
            "amplitude_analyt": row['amplitude'] if row['Channel'] == 'Analyt' else None
        }
        output_dict["pulses"].append(pulse_dict)
    
    # Add the current step's output to the final output dictionary
    final_output[f"step_{step_value}"] = output_dict

print(final_output)