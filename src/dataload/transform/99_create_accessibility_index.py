import pandas as pd
from pathlib import Path
import numpy as np

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'

df_commute = pd.read_csv(PROCESSED / 'commute_data.csv')
df_ttm = pd.read_csv(PROCESSED / 'travel_time_dist/all.csv')

df_ttm_m = pd.merge(df_ttm, df_commute, left_on=['fromId','toId'], right_on=['lsoa_home', 'lsoa_work'], how='inner')
df_ttm_m = df_ttm_m.drop(columns=['fromId','toId'])

# Some commuters will go outside of the two hours, so need to re-normalise
df_ttm_m['lsoa_home_total'] = df_ttm_m.groupby('lsoa_home')['count'].transform('sum')
df_ttm_m['destination_demand_commute'] = df_ttm_m['count'] / df_ttm_m['lsoa_home_total']
assert (df_ttm_m.groupby('lsoa_home')['destination_demand_commute'].sum().round(10) == 1).all()

# add population data
df_census = pd.read_csv(PROCESSED / 'census_stats.csv')
df_ttm_m = pd.merge(
    df_ttm_m, df_census[['lsoa','population']], 
    left_on='lsoa_work', right_on='lsoa', how='left'
).drop(columns=['lsoa'])
df_ttm_m = df_ttm_m.rename(columns={'population': 'destination_population'})
assert df_ttm_m.destination_population.isna().sum() == 0

# Need to normalise the population to one, otherwise places that 
# are near to more populous places will have a higher index
## TODO: To discuss, is this index appropriate when there are 
# different numbers of destinations from each LSOA?
df_ttm_m['destination_population_total'] = df_ttm_m.groupby('lsoa_home')['destination_population'].transform('sum')
df_ttm_m['destination_demand_population'] = df_ttm_m['destination_population'] / df_ttm_m['destination_population_total']

# df_ttm_m['destination_population'] = np.random.randint(500, 1000, df_ttm_m.shape[0])
df_ttm_m['destination_jobs'] = np.random.randint(10, 10000, df_ttm_m.shape[0])
df_ttm_m['beta'] = 0.5

# Create indices using PT time
df_ttm_m['index_conn_commute_pt'] = \
    df_ttm_m['destination_demand_commute'] \
    * np.exp(-df_ttm_m['beta'] * df_ttm_m['travel_time_p025'])

df_ttm_m['index_conn_pop_pt'] = \
    df_ttm_m['destination_demand_population'] \
    * np.exp(-df_ttm_m['beta'] * df_ttm_m['travel_time_p025'])

df_ttm_m['index_conn_jobs_pt'] = \
    df_ttm_m['destination_jobs'] \
    * np.exp(-df_ttm_m['beta'] * df_ttm_m['travel_time_p025'])

# Then the same with crow flies travel time
df_ttm_m['travel_time_crow'] = df_ttm_m['dist_km'] / 50 * 60

df_ttm_m['index_conn_commute_cf'] = \
    df_ttm_m['destination_demand_commute'] \
    * np.exp(-df_ttm_m['beta'] * df_ttm_m['travel_time_crow'])
df_ttm_m['index_conn_pop_cf'] = \
    df_ttm_m['destination_demand_population'] \
    * np.exp(-df_ttm_m['beta'] * df_ttm_m['travel_time_crow'])
df_ttm_m['index_conn_jobs_cf'] = \
    df_ttm_m['destination_jobs'] \
    * np.exp(-df_ttm_m['beta'] * df_ttm_m['travel_time_crow'])

df_ttm_index = df_ttm_m.groupby('lsoa_home')[[
    'index_conn_commute_pt', 'index_conn_pop_pt', 'index_conn_jobs_pt',
    'index_conn_commute_cf', 'index_conn_pop_cf', 'index_conn_jobs_cf'
]].sum().reset_index()

# Normalise each index
for col_prefix in ['index_conn_commute', 'index_conn_pop', 'index_conn_jobs']:
    df_ttm_index[f"{col_prefix}_norm"] = df_ttm_index[f"{col_prefix}_pt"] / df_ttm_index[f"{col_prefix}_cf"].max()

df_ttm_index.to_csv(PROCESSED / 'accessibility_index.csv', index=False)


# there are some values of 1 for pt and crow flies times - why is that?
# E01001412
# E01019077
# E01019131
# E01019325
# E01019337
# E01030346


## these results don't seem all that realistic - need to check the data 