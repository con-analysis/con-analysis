# Combine gov data with supermarket data

import pandas as pd
from pathlib import Path
import numpy as np
import json
from shapely.geometry import shape, Point
from tqdm import tqdm
import folium

from pandarallel import pandarallel
# confirm that missing LSOA locations are not in England?


ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'
ANALYSIS = ROOT / 'data' / 'analysis'

folder = PROCESSED / "destinations/lsoa"
supermarkets = list(folder.glob("supermarkets*.csv"))
df_supermarkets = pd.concat([pd.read_csv(f) for f in supermarkets])
df_supermarkets = df_supermarkets[['lsoa', 'lat', 'lon', 'service_type']]

# ROUGH CHECK TO SHOW THAT NULL SUPERMARKET VALUES ARE OUTSIDE ENGLAND
# Came from https://geoportal.statistics.gov.uk/datasets/5a393192a58a4e50baf87eb4d64ca828_0/explore?location=54.958209%2C-3.317654%2C6.11
# GB_GEOJSON = RAW / "NUTS1_Jan_2018_SGCB_in_the_UK_2022_1573160115369314421.geojson"
# geojson = json.load(open(GB_GEOJSON))
# df_null = df_supermarkets[df_supermarkets["lsoa"].isnull()] 

# m = folium.Map([55, 0], zoom_start=8)
# folium.TileLayer('cartodbpositron').add_to(m)

# for coord in df_null[['lat', 'lon']].values:
#     folium.Marker( location=[ coord[0], coord[1] ], fill_color='#43d9de', radius=8 ).add_to( m )
# m.save(OUT / "supermarkets_missing_lsoa.html")

df_supermarkets = df_supermarkets.dropna(subset=['lsoa'])
other_services = list(folder.glob("services*.csv"))
df_other_services = pd.concat([pd.read_csv(f) for f in other_services])

df_long = pd.concat([df_supermarkets, df_other_services])
df_wide = df_long.pivot_table(
    index='lsoa', columns='service_type', values='lat', 
    aggfunc='count'
).fillna(0).reset_index()
df_long['lsoa'] = df_long['lsoa'].str.replace(' ', '')
df_long['count'] = 1

df_wide = df_long.pivot_table(
    index='lsoa', columns='service_type', values='count', aggfunc='count'
).fillna(0).reset_index()

# Add centroid coordinates
df_pwc = pd.read_csv(PROCESSED / 'lsoa_pwc.csv')
df_pwc = df_pwc[['lsoa', 'lat', 'lon']]
df_wide = pd.merge(
    df_wide, df_pwc, on='lsoa', how='left'
)

df_wide.to_csv(ANALYSIS / 'services_by_lsoa.csv', index=False)