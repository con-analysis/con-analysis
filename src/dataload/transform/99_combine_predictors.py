import pandas as pd
from pathlib import Path
import numpy as np

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'
CENSUS = RAW / 'census'

df_tt_dft = pd.read_csv(PROCESSED / 'travel_times_dft.csv')
df_census = pd.read_csv(PROCESSED / 'census_stats.csv')
df_access_index = pd.read_csv(PROCESSED / 'accessibility_index.csv')

df_town_centre = df_tt_dft.loc[
    (df_tt_dft.service_type == 'town_centre') & (df_tt_dft.year == 2019),
    ['lsoa', 'travel_time_car', 'travel_time_pt'] 
]
df_town_centre = df_town_centre.rename(
    columns={
        'travel_time_car': 'travel_time_car_town_centre', 
        'travel_time_pt': 'travel_time_pt_town_centre'
    }
)

df_comb = pd.merge(df_town_centre, df_census, on='lsoa')
df_comb['urban'] = df_comb.rural_urban_category.str.contains('Urban')

# Add in accessibility index
df_ttm = pd.read_csv(PROCESSED / 'accessibility_index.csv')
df_comb = pd.merge(df_comb, df_ttm, left_on='lsoa', right_on='lsoa_home', how='left')
df_comb = df_comb.drop(columns=['lsoa_home'])

df_comb.to_csv(PROCESSED / 'combined_predictors.csv', index=False)
