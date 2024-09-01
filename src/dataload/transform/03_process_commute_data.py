"""
2021 commuter data - TBD - Probably need to update this to use 2011 commuter data...
Covid effect
"""

import pandas as pd
from pathlib import Path
import numpy as np

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'


# Clean the commuting data
df_commute = pd.read_csv(RAW / 'odwp01ew' / 'ODWP01EW_OA.csv')
df_commute = df_commute.rename({'Output Areas code':'oa_home', 'OA of workplace code': 'oa_work', 
                      'Place of work indicator (4 categories) code':'workplace_code', 'Count':'count'}, axis=1)
df_commute = df_commute[['oa_home', 'oa_work', 'workplace_code', 'count']]

# Shoul I exclude work from home people...?
workplace_code_map = {
    1: 'Work mainly at or from home',
    2: 'Other. Including outside the UK',
    3: 'Working in the UK but not from home',
    -8: 'Does not apply (?)'
}

# Drop workers outside the UK or not working
# Drop WFH people, only keep people who travel to work (either to the same or another OA)
    # we see from assert that there are people who travel to work in the same OA, so WFH can be validly
    # excluded
assert not df_commute.loc[(df_commute.oa_home == df_commute.oa_work) & (df_commute.workplace_code == 3)].empty
df_commute = df_commute.loc[df_commute.workplace_code.isin([3])]

# Merge on home
df_oa_lookup = pd.read_csv(PROCESSED / 'oa_lsoa_21_lookup.csv')
df_commute = df_commute.merge(df_oa_lookup, left_on='oa_home', right_on='oa', how='left', indicator=True)
assert (df_commute[df_commute._merge=='left_only'].oa_home.str.startswith('W')).all() # all Welsh if not merged correctly
df_commute = df_commute.loc[df_commute._merge=='both'].drop(columns=['_merge', 'oa', 'oa_home'])
df_commute = df_commute.rename({'lsoa': 'lsoa_home'}, axis=1)

# Merge on work
df_commute = df_commute.merge(df_oa_lookup, left_on='oa_work', right_on='oa', how='left', indicator=True)
assert df_commute[df_commute._merge=='left_only'].oa_work.str.startswith(('W', 'S', 'N')).all() # Exclude Scotland, Wales, NI. At most there are 42 people going from one to the other
df_commute = df_commute.loc[df_commute._merge=='both'].drop(columns=['_merge', 'oa', 'oa_work'])
df_commute = df_commute.rename({'lsoa': 'lsoa_work'}, axis=1)

# Sum by LSOA
df_commute = df_commute.groupby(['lsoa_home', 'lsoa_work'])['count'].sum().reset_index()

# Get total columns
df_commute['lsoa_home_total'] = df_commute.groupby('lsoa_home')['count'].transform('sum')
df_commute['lsoa_work_total'] = df_commute.groupby('lsoa_work')['count'].transform('sum')

df_commute.to_csv(PROCESSED / 'commute_data.csv', index=False)