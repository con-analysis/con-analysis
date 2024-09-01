# Get pairwise distances between LSOAs within a defined radius


import pandas as pd
from pathlib import Path
import numpy as np
from shapely.geometry import shape, Point
from tqdm import tqdm
# from scipy.spatial import KDTree
from sklearn.neighbors import BallTree
import polars as pl

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'
ANALYSIS = ROOT / 'data' / 'analysis'

df_pwc = pl.read_csv(PROCESSED / 'lsoa_pwc.csv')
df_pwc = df_pwc.drop(columns=['x', 'y'])

bt = BallTree(np.deg2rad(df_pwc[['lat', 'lon']]), metric='haversine')

query_lats = list(df_pwc[:, 'lat'])
query_lons = list(df_pwc[:, 'lon'])

# Assumes a sphere of radius 1, earth radius is 6371
DIST_MAX_KM = 2
EARTH_RADIUS = 6371
indices, distances = bt.query_radius(
    np.deg2rad(np.c_[query_lats, query_lons]), 
    r=DIST_MAX_KM/EARTH_RADIUS, 
    return_distance=True
)
distances_km = [d * 6371 for d in distances]

df_destinations_full = pl.DataFrame()
for i in tqdm(range(len(indices))):
    df_destinations = df_pwc[indices[i], ['lsoa', 'lat', 'lon']].rename(
        {'lsoa': 'lsoa_destination', 'lat': 'lat_destination', 'lon': 'lon_destination'}
    )
    df_destinations = df_destinations.with_columns(pl.Series(name="distance_km", values=distances_km[i].round(1)))
    df_destinations = df_destinations.with_columns(df_pwc[i, ['lsoa', 'lat', 'lon']])
    df_destinations = df_destinations[['lsoa', 'lsoa_destination', 'distance_km']]
    df_destinations = df_destinations.rename({'lsoa': 'lsoa_origin'})
    df_destinations_full = df_destinations_full.vstack(df_destinations)

df_destinations_full.write_csv(ANALYSIS / f"distances_between_lsoa_{DIST_MAX_KM}.csv")