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
print(impulses.head)

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
    final_output[str(f"step_{step_value}")] = output_dict
# with open('final_output.json', 'w') as file:
#     json.dump(final_output, file, separators=(',', ':'))