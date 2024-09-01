import pandas as pd
import numpy as np
from shapely.geometry import shape, Point
from tqdm import tqdm
import pyproj

# from scipy.spatial import KDTree
from sklearn.neighbors import BallTree
from scipy.spatial import distance

# from geopy.distance import distance as geodist
from geopy.distance import geodesic
import geopy.distance

import geopandas as gpd

from src.constants import RAW, PROCESSED, OUT, ANALYSIS

# CREATE BUA CENTROIDS
p = "Built_up_Areas_Dec_2011_Boundaries_V2_2022_-5874362614624520768.geojson"
gdf_bua = gpd.read_file(RAW / p)
gdf_bua.to_crs(epsg=4326, inplace=True)

## mapping of bua to pua
pua = pd.read_csv(RAW / "PUA-BUA.csv")

## Add centroids - filter to only PUA
gdf_bua["c"] = gdf_bua.centroid
gdf_pua = gdf_bua[gdf_bua.BUA11CD.isin(pua["built-up area code"])]

### does it matter if the PUA is not contained within its BUA boundary?
### Actually, no it doesnt... as long as it's in england
gdf_pua["geometry"].contains(gdf_pua["c"]).all()

### Move London to centroid to a place that makes more sense
gdf_pua.loc[gdf_pua.BUA11CD == "E34004707", "c"] = Point(
    -0.11222031381823115, 51.518239079121955
)
gdf_pua = gdf_pua.drop(columns="geometry")
gdf_pua.set_geometry("c", inplace=True)

# CREATE DISTANCE DATAFRAMES
msoa = gpd.read_file(PROCESSED / "msoa_shape.geojson")
gdf = gpd.read_file(PROCESSED / "road_rail_locations.geojson", driver="GeoJSON")

centroids = msoa.centroid
centroids_l = [(c.y, c.x) for c in centroids]

## Create locations for rail, road, pua
rail_locs = [(l.y, l.x) for l in gdf[gdf["place_type"] == "rail"].geometry]
road_locs = [(l.y, l.x) for l in gdf[gdf["place_type"] == "road"].geometry]
pua_locs = [(l.y, l.x) for l in gdf_pua.geometry]

# Distance between all centroids and all rail, road, pua locations
dists_rail = distance.cdist(
    centroids_l, rail_locs, lambda u, v: geodesic(u, v).kilometers
)
dists_road = distance.cdist(
    centroids_l, road_locs, lambda u, v: geodesic(u, v).kilometers
)
dists_pua = distance.cdist(
    centroids_l, pua_locs, lambda u, v: geodesic(u, v).kilometers
)

# Create a dataframe with the minimum distances
df = pd.DataFrame(msoa["msoa"])
df["min_dist_rail"] = np.min(dists_rail, axis=1)
df["min_dist_road"] = np.min(dists_road, axis=1)
df["min_dist_pua"] = np.min(dists_pua, axis=1)
df.to_csv(PROCESSED / "distances_to_rail_road3.csv", index=False)
