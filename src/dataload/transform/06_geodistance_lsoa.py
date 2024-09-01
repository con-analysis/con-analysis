## Get distances between a list of LSOAs - atm the list 
# comes from the public accessibility inddicator data

import pandas as pd
from pathlib import Path
import numpy as np
import geopy.distance
from tqdm import tqdm
from pandarallel import pandarallel

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'

def get_distances(row):
    coords_1 = (row['from_lat'], row['from_lon'])
    coords_2 = (row['to_lat'], row['to_lon'])
    dist = geopy.distance.distance(coords_1, coords_2).km
    row['dist_km'] = dist
    return row

df_ttm = pd.read_csv(PROCESSED / 'travel_times_ttm.csv')
df_lsoa_pwc = pd.read_csv(PROCESSED / 'lsoa_pwc.csv')
df_lsoa_pwc = df_lsoa_pwc[['lsoa', 'lat', 'lon']]
df_lsoa_pwc[['lat', 'lon']] = df_lsoa_pwc[['lat', 'lon']].round(4)

# Add lat lon to ttm
df_ttm = pd.merge(df_ttm, df_lsoa_pwc, left_on='fromId', right_on='lsoa', how="left")
df_ttm = df_ttm.rename(columns={'lat': 'from_lat', 'lon': 'from_lon'})
df_ttm = df_ttm.drop(columns=['lsoa'])

df_ttm = pd.merge(df_ttm, df_lsoa_pwc, left_on='toId', right_on='lsoa', how="left")
df_ttm = df_ttm.rename(columns={'lat': 'to_lat', 'lon': 'to_lon'})
df_ttm = df_ttm.drop(columns=['lsoa'])


def chunker(seq, size):
    for pos in range(0, len(seq), size):
        yield seq.iloc[pos:pos + size] 


N_WORKERS = 8
i = 0
chunk_size = int(len(df_ttm) / 10)
for chunk in chunker(df_ttm, chunk_size):
    i += 1
    pandarallel.initialize(progress_bar=True, nb_workers=N_WORKERS-2)
    chunk = chunk.parallel_apply(get_distances, axis=1)
    chunk['dist_km'] = chunk['dist_km'].round(2)
    chunk = chunk.drop(columns=['from_lat', 'from_lon', 'to_lat', 'to_lon'])
    chunk.to_csv(PROCESSED / f'travel_time_dist/travel_times_ttm_w_distance_{i}.csv', index=False)
    del chunk