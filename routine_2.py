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
    'Impulse': [0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
    'Channel': ['Reper', 'Analyt', 'Reper', 'Analyt', 'Reper', 'Analyt', 'Reper', 'Analyt', 'Reper', 'Analyt', 'Reper', 'Analyt'],
    'step': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  
    'amplitude': [100, 1.6, 150, 2.2, 200, 2.2, 100, 1.6, 150, 2.2, 200, 2.2],  
    'array_element_1': [1.5, 1.6, 2.1, 2.2, 2.0, 2.2, 7.894, 3.678, 5.234, 9.123, 0.567, 4.321],
    'array_element_2': [1.5, 1.6, 2.1, 2.2, 2.0, 2.2, 7.894, 3.678, 5.234, 9.123, 0.567, 4.321],
    'array_element_3': [1.5, 1.6, 2.1, 2.2, 2.0, 2.2, 7.894, 3.678, 5.234, 9.123, 0.567, 4.321],
    'array_element_n': [1.5, 1.6, 2.1, 2.2, 2.0, 2.2, 7.894, 3.678, 5.234, 9.123, 0.567, 4.321]
}
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