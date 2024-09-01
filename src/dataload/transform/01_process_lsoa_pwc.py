import pandas as pd
from pathlib import Path
import numpy as np
import pyproj
from tqdm import tqdm

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'

# Convert eastings, northing to lat, long
crs_british = pyproj.Proj(init='EPSG:27700')
crs_wgs84 = pyproj.Proj(init='EPSG:4326')
def en_to_lon_lat(row):
    lon, lat = pyproj.transform(crs_british, crs_wgs84, row['x'], row['y'])
    return lat, lon

df_lsoa_pwc = pd.read_csv(PROCESSED / 'lsoa_pwc_en.csv')

tqdm.pandas()
df_lsoa_pwc[['lat', 'lon']] = df_lsoa_pwc.progress_apply(
    lambda row: en_to_lon_lat(row), axis=1, result_type='expand'
)
df_lsoa_pwc[['lat', 'lon']] = df_lsoa_pwc[['lat', 'lon']].round(4)
df_lsoa_pwc.to_csv(PROCESSED / 'lsoa_pwc.csv', index=False)