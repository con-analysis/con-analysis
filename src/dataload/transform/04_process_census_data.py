import pandas as pd
from pathlib import Path
import numpy as np

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'
CENSUS = RAW / 'census'

# First add LSOA region changes and drop the 10 that have changed irregularly
df_lsoa_lookup = pd.read_csv(PROCESSED / 'lsoa_lookup.csv')
df_lsoa_unchanged = df_lsoa_lookup.loc[df_lsoa_lookup.CHGIND == 'U', 'lsoa_11'].unique()
df_lsoa_merged = df_lsoa_lookup.loc[df_lsoa_lookup.CHGIND == 'M', 'lsoa_11'].unique()
df_lsoa_split = df_lsoa_lookup.loc[df_lsoa_lookup.CHGIND == 'S', 'lsoa_11'].unique()
df_lsoa_2011_keep = list(df_lsoa_lookup.lsoa_11.unique())
df_lsoa_2021_keep = list(df_lsoa_lookup.lsoa_21.unique())

# Population stats
df_pop = pd.read_csv(CENSUS / 'census2021-age-bands.csv')
df_pop = df_pop[['geography code', 'Age: Total']]
df_pop.columns = ['lsoa', 'population']
df_pop = df_pop.loc[df_pop.lsoa.isin(df_lsoa_2021_keep)]

# AREA STATS
df_area = pd.read_csv(RAW / 'SAM_LSOA_DEC_2021_EW_in_KM.csv')
df_area = df_area[['LSOA21CD', 'Land Count (Area in KM2)']]
df_area.columns = ['lsoa', 'area']
df_area = df_area.loc[df_area.lsoa.isin(df_lsoa_2021_keep)]

# Combine
df_pd = pd.merge(df_pop, df_area, on='lsoa')
df_pd = df_pd.loc[df_pd.lsoa.str.startswith('E')]
df_pd['pop_density'] = df_pd['population'] / df_pd['area']


# Add rural urban classification
df_rural = pd.read_csv(RAW / 'Rural_Urban_Classification_(2011)_of_Lower_Layer_Super_Output_Areas_in_England_and_Wales.csv')
df_rural = df_rural[['LSOA11CD', 'RUC11']]
df_rural.columns = ['lsoa_11', 'rural_urban_category']

# Add/change lsoa_11 to lsoa_21
## Unchanged cases
df_rural_21 = df_rural.loc[df_rural.lsoa_11.isin(df_lsoa_unchanged)]
df_rural_21 = df_rural_21.rename(columns={'lsoa_11': 'lsoa_21'})

## Split cases
df_split_lsoa_21 = df_lsoa_lookup.loc[df_lsoa_lookup.lsoa_11.isin(df_lsoa_split)]
df_split_lsoa_21 = df_rural.merge(df_split_lsoa_21, on='lsoa_11', how='right')
df_split_lsoa_21 = df_split_lsoa_21[['lsoa_21', 'rural_urban_category']]
df_rural_21 = pd.concat([df_rural_21, df_split_lsoa_21])

## Merged cases
df_m_lsoa_21 = df_lsoa_lookup.loc[df_lsoa_lookup.lsoa_11.isin(df_lsoa_merged)]
df_m_lsoa_21 = df_rural.merge(df_m_lsoa_21, on='lsoa_11', how='right')

merged_with_category_change = df_m_lsoa_21[~df_m_lsoa_21.duplicated(subset=['rural_urban_category', 'lsoa_21'], keep=False)]
assert len(merged_with_category_change) != 0
df_m_lsoa_21 = df_m_lsoa_21.drop_duplicates(subset=['lsoa_21'], keep='last')
df_m_lsoa_21 = df_m_lsoa_21[['lsoa_21', 'rural_urban_category']]
df_rural_21 = pd.concat([df_rural_21, df_m_lsoa_21]).reset_index(drop=True)
df_rural_21 = df_rural_21.rename(columns={'lsoa_21': 'lsoa'})
df_combined = pd.merge(df_pd, df_rural_21, on='lsoa')

df_combined.to_csv(PROCESSED / 'census_stats.csv', index=False)