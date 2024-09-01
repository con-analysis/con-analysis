"""Process accessibility indicators data. This is the number of services
that are accessibly within a public transport time frame.

Data are from here: https://data.ubdc.ac.uk/datasets/public-transport-accessibility-indicators-2022/resource/fb8bc3f7-112a-475e-a9b8-8496c78be9ff

This is mostly superceded - public transport time was not really appropriate...

"""


import pandas as pd
from pathlib import Path

# WHICH YEAR DO THESE LSOA CODES BELONG TO?


ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
PATH = RAW / 'accessibility_indicators_gb' / 'accessibility'

folder_names = [dir.stem for dir in list(PATH.glob("*")) if dir.is_dir()]
FILE_NAME_PATTERN = "access_{service_type}_pt.csv"
file_names = [PATH / folder_name / FILE_NAME_PATTERN.format(service_type=folder_name) for folder_name in folder_names]
file_names = [fn for fn in file_names if fn.stem != 'access_cities_pt']

# Read in the CSVs
df_list = [pd.read_csv(file_name) for file_name in file_names]

# Checks
assert(all([df.shape[0] == 41729 for df in df_list]))
lsoa = df_list[0].geo_code
assert(all([df.geo_code.equals(lsoa) for df in df_list]))

# Split school dfs into primary and secondary
school_mask = ['school' in df.columns[2] for df in df_list]
school_i = [i for i, x in enumerate(school_mask) if x][0]
df_school = df_list.pop(school_i)

df_primary = df_school.loc[:, ~df_school.columns.str.contains('secondary')]
df_primary.columns = [col.split('_', maxsplit=1)[1] if 'school' in col else col for col in df_primary.columns ]
df_secondary = df_school.loc[:, ~df_school.columns.str.contains('primary')]
df_secondary.columns = [col.split('_', maxsplit=1)[1] if 'school' in col else col for col in df_secondary.columns ]
df_list = df_list + [df_primary, df_secondary]

def filter_cols(df):
    
    df = df.drop(list(df.filter(regex = 'pct')), axis = 1)
    df = df.drop(list(df.filter(regex = 'nearest')), axis = 1)
    df = df.drop('geo_label', axis=1)
    df = df.rename({'geo_code': 'lsoa'}, axis=1)
    df = df.loc[df.lsoa.str.startswith('E')]
    df_numbers = df.loc[:, df.columns.str.contains('|'.join(['15', '30', '45', '60']))]
    service_type = df_numbers.columns[0].split('_')[0]
    df_numbers.columns = ['number_pt_' + colname[-2:] for colname in df_numbers.columns]
    df = pd.concat([df['lsoa'], df_numbers], axis=1)
    df['service_type'] = service_type
    
    assert(df.shape[0] == 32844)
    return df

full_df = filter_cols(df_list[0])
for sub_df in df_list[1:]:
    sub_df = filter_cols(sub_df)    
    full_df = pd.concat([full_df, sub_df], ignore_index=True)

assert full_df.shape[0] == 32844 * len(df_list)
full_df = full_df.replace({'service_type': {'hospitals': 'hospital'}})
full_df.to_csv(PROCESSED / 'service_accessibility.csv', index=False)
