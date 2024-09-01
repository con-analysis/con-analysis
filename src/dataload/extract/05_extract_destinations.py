import pandas as pd
from pathlib import Path
import numpy as np

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
TEMP = ROOT / 'data' / 'temp' / 'destinations'

# data came from https://www.gov.uk/government/publications/journey-time-statistics-guidance#:~:text=This%20file%20is%20in%20an%20OpenDocument%20format
PATH = RAW / 'journey-time-statistics-2019-destination-datasets (2).ods'

"""
Small employment centres	17,457
Medium employment centres	10,545
Large employment centres	843
Primary schools	16,948
Secondary schools	3,128
Further education	2,198
GPs	6,866
Hospitals	219
Food stores	23,161 (From a commercial dataset - had to recreate my own)
Town centres	1,211
"""



# TODO: Adjust this so it doesn't need the temp directory - hacky because I was adding smth quickly
data = pd.ExcelFile(PATH)
relevant_sheets = [name for name in data.sheet_names if name != 'Cover_sheet']
df_list = []
for sheet in relevant_sheets:
    df = pd.read_excel(PATH, sheet_name=sheet, skiprows=2)
    TEMP.mkdir(parents=True, exist_ok=True)
    df.to_csv(TEMP /  f'{sheet}.csv', index=False)

NAME_MAP = {
    'Secondary_schools': 'secondary',
    'Primary_schools': 'primary',
    'Large_employment_centres': 'emp',
    'Small_employment_centres': 'employment_small',
    'Medium_employment_centres': 'employment_medium',
    'Town_centres': 'town_centre',
    'GPs': 'gp',
    'Hospitals': 'hosp',
    'Further_education_': 'further_ed',
}
cols = {'LSOACode': 'lsoa', 'Easting':'easting', 'Northing':'northing'}
sheets = list(TEMP.glob('*.csv')  )


## merge all the sheets in to a single csv and save that
df_full = pd.DataFrame(columns=cols.values())
for sheet in sheets:
    name = sheet.stem
    
    # Only use large employment centres for now - think that's what DFT does
    if name in ["Small_employment_centres", "Medium_employment_centres"]: continue
    df = pd.read_csv(sheet)

    df = df[[col for col in df.columns if col in cols.keys()]]
    if 'LSOACode' not in df.columns:
        df['LSOACode'] = ""
    df['service_type'] = NAME_MAP[name]
    df = df.rename(columns=cols)
    df_full = pd.concat([df_full, df])
    df_full.to_csv(PROCESSED / 'destinations/destinations.csv', index=False)

print("05 run successfully")