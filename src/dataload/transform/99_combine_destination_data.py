import pandas as pd
from pathlib import Path

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'

df = pd.read_csv(PROCESSED / 'service_accessibility.csv')
df_tt = pd.read_csv(PROCESSED / 'travel_times_dft.csv')

df_town_centre = df_tt.loc[(df_tt.year == 2019) & (df_tt.service_type == 'town_centre'), ['lsoa', 'number_pt_15', 'number_pt_30', 'number_pt_45', 'number_pt_60', 'service_type']]
df = pd.concat([df, df_town_centre], axis=0)

df.to_csv(PROCESSED / 'destination_data.csv', index=False)