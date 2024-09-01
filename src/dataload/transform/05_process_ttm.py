## PROCESS public transport travel times between LSOAs within 2 hour radius
# this was from public accessibility indicators data

import pandas as pd
from pathlib import Path
import numpy as np

import seaborn as sns
import matplotlib.pyplot as plt
from tqdm import tqdm

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'

df_lsoa_lookup = pd.read_csv(PROCESSED / 'lsoa_lookup.csv')
df_lsoa_unchanged = df_lsoa_lookup.loc[df_lsoa_lookup.CHGIND == 'U', 'lsoa_11'].unique()
df_lsoa_merged = df_lsoa_lookup.loc[df_lsoa_lookup.CHGIND == 'M', 'lsoa_11'].unique()
df_lsoa_split = df_lsoa_lookup.loc[df_lsoa_lookup.CHGIND == 'S', 'lsoa_11'].unique()
df_lsoa_2011_keep = list(df_lsoa_lookup.lsoa_11.unique())
df_lsoa_2021_keep = list(df_lsoa_lookup.lsoa_21.unique())

def update_lsoa_multiple(df):
    df = update_lsoa(df, to_from='from')
    df = update_lsoa(df, to_from='to')  
    df = df[['fromId', 'toId', 'travel_time_p025', 'travel_time_p050', 'travel_time_p075']]
    return df

def update_lsoa(df, to_from='from'):
    tqdm.pandas()
    otherId = 'toId' if to_from == 'from' else 'fromId'
    df1 = df.merge(df_lsoa_lookup, left_on=f'{to_from}Id', right_on='lsoa_11', how='left').progress_apply(lambda x: x)
    df2 = df1[['lsoa_21', otherId, 'travel_time_p025', 'travel_time_p050', 'travel_time_p075']]
    df2 = df2.rename(columns={'lsoa_21': f'{to_from}Id'})
    # df2 = df2.sort_values(by=['fromId', 'toId'])
    df2 = df2.groupby(['fromId', 'toId']).mean().reset_index()
    return df2

df_ttm = pd.read_csv(RAW / 'accessibility_indicators_gb/ttm/ttm_pt_20211122.csv')
df_ttm_updated = update_lsoa_multiple(df_ttm)
df_ttm_updated.to_csv(PROCESSED / 'travel_times_ttm.csv', index=False)