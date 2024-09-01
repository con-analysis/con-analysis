# For services defined by gov - find the LSOA for the lat lon pair

# There are many stops with missing lon lat - need to convert from Easting and Northing
# which are complete :/

import pandas as pd
from pathlib import Path
import numpy as np
import json
from shapely.geometry import shape, Point
from tqdm import tqdm
import pyproj
from tqdm import tqdm
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

from pandarallel import pandarallel

pd.options.mode.chained_assignment = None  # default='warn'

ROOT = Path.cwd()
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
OUT = ROOT / "out"
ANALYSIS = ROOT / "data" / "analysis"

geojson = json.load(open(PROCESSED / "msoa_shape.geojson"))
df = pd.read_csv(PROCESSED / "public_transport_stops.csv", index_col=0)

# Convert eastings, northing to lat, long
crs_british = pyproj.Proj(init="EPSG:27700")
crs_wgs84 = pyproj.Proj(init="EPSG:4326")


def en_to_lon_lat(row):
    lon, lat = pyproj.transform(crs_british, crs_wgs84, row["Easting"], row["Northing"])
    return lat, lon


# convert Easting and Northing to lat, lon if no lat lon available
# first null row is 19080
def get_msoa(row):

    if np.isnan(row.lat) or np.isnan(row.lon):
        lat, lon = en_to_lon_lat(row)
        row["lat"] = lat
        row["lon"] = lon

    point = Point(row.lon, row.lat)
    for area in geojson["features"]:
        polygon = shape(area["geometry"])
        if polygon.contains(point):
            row.loc["msoa"] = area["properties"]["msoa"]
            return row
        pass

    return row


def chunker(seq, size):
    for pos in range(0, len(seq), size):
        yield seq.iloc[pos : pos + size]


N_WORKERS = 8
i = 0
chunk_size = int(len(df) / 10)
# tqdm.pandas()
for chunk in chunker(df, chunk_size):
    i += 1
    if i < 9:
        continue
    print(i)
    pandarallel.initialize(progress_bar=True, nb_workers=4)
    chunk = chunk.parallel_apply(get_msoa, axis=1)
    # chunk = chunk.progress_apply(get_msoa, axis=1)
    chunk.to_csv(PROCESSED / f"public_transport/msoa/locations_{i}.csv", index=False)
    del chunk


# # df.iloc[19075:19105].apply(get_msoa, axis=1)
# # df.iloc[23810:23820].apply(get_msoa, axis=1)

# en_to_lon_lat(df.iloc[19080])

# np.isnan(df.iloc[19080].lat)
# df.iloc[23810:23820]
