# For services data from gov, convert eastings, northings to lat, long

import pandas as pd
from pathlib import Path
import numpy as np
import pyproj
from tqdm import tqdm

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'
ANALYSIS = ROOT / 'data' / 'analysis'

# Convert eastings, northing to lat, long
crs_british = pyproj.Proj(init='EPSG:27700')
crs_wgs84 = pyproj.Proj(init='EPSG:4326')
def en_to_lon_lat(row):
    lon, lat = pyproj.transform(crs_british, crs_wgs84, row['easting'], row['northing'])
    return lat, lon

df = pd.read_csv(PROCESSED / 'destinations' / 'destinations.csv')

tqdm.pandas()
df[['lat', 'lon']] = df.progress_apply(
    lambda row: en_to_lon_lat(row), axis=1, result_type='expand'
)
df[['lat', 'lon']] = df[['lat', 'lon']].round(4)
df = df[['lsoa', 'service_type', 'lat', 'lon']]
df.to_csv(PROCESSED / 'destinations/services_lat_lon.csv', index=False)