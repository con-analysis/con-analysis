# Supermarket data is not given by gov website, so instead use the data from
# https://geolytix.com/blog/supermarket-retail-points/ (extract 06)
# find LSOA for these

import pandas as pd
from pathlib import Path
import numpy as np
import json
from shapely.geometry import shape, Point
from tqdm import tqdm

from pandarallel import pandarallel

pd.options.mode.chained_assignment = None  # default='warn'

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'
ANALYSIS = ROOT / 'data' / 'analysis'

geojson = json.load(open(RAW / 'lsoa.geojson'))
assert "LSOA21CD" in geojson['features'][0]['properties'].keys() # therefore 2021

path = PROCESSED / 'supermarkets_lat_lon.csv'
df = pd.read_csv(path)
df = df[['lsoa', 'lat', 'lon', 'service_type', 'county', 'town', 'fascia']]
# df = df.iloc[:1000]

def get_lsoa(row):
    
    # if already has lsoa, return # won't be the case for the supermarkets
    # if not pd.isnull(row['lsoa']):
    #     return row
    
    point = Point(row.lon, row.lat)
    for area in geojson['features']:
        polygon = shape(area['geometry'])
        if polygon.contains(point):
            row.loc['lsoa'] = area['properties']['LSOA21CD']
            return row
        pass

    return row

# tqdm.pandas()
# df = df.progress_apply(
#     lambda row: get_lsoa(row), axis=1
# )

# N_WORKERS = 6
# pandarallel.initialize(progress_bar=True, nb_workers=N_WORKERS)
# df = df.parallel_apply(get_lsoa, axis=1)
# df.to_csv(PROCESSED / 'destinations/lsoa/supermarkets.csv', index=False)


def chunker(seq, size):
    for pos in range(0, len(seq), size):
        yield seq.iloc[pos:pos + size] 


N_WORKERS = 8
i = 0
chunk_size = int(len(df) / 10)
for chunk in chunker(df, chunk_size):
    i += 1
    pandarallel.initialize(progress_bar=True, nb_workers=N_WORKERS-2)
    chunk = chunk.parallel_apply(get_lsoa, axis=1)
    # chunk = chunk.drop(columns=['from_lat', 'from_lon', 'to_lat', 'to_lon'])
    chunk.to_csv(PROCESSED / f'destinations/lsoa/supermarkets_{i}.csv', index=False)
    del chunk