# For services defined by gov - find the LSOA for the lat lon pair

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

path = PROCESSED / "destinations/services_lat_lon.csv"
df = pd.read_csv(path)
df = df[['lsoa', 'lat', 'lon', 'service_type']]
# df = df.iloc[3000:4000]

def get_lsoa(row):
    
    ## EDITED - Don't do this - employment centres use 2011 boundaries so are
    ## outdated
    # if already has lsoa, return 
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
# df.to_csv(ANALYSIS / 'services_lsoa.csv', index=False)


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
    chunk.to_csv(PROCESSED / f'destinations/lsoa/services_{i}.csv', index=False)
    del chunk