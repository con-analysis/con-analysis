import pandas as pd
from pathlib import Path
import numpy as np
import geopandas as gpd
from tqdm import tqdm

ROOT = Path.cwd()
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
OUT = ROOT / "out"
CENSUS = RAW / "census"

p = RAW / "oproad_essh_gb/data"
files = sorted(p.glob("*dbf"))
list(files)
road_nodes = [f for f in files if "Node" in str(f)]
# road_nodes = [f for f in files if "Link" in str(f)]
len(road_nodes)

gdf = gpd.read_file(road_nodes[0])
gdf = gdf.loc[gdf["formOfNode"] == "junction"]
for file in tqdm(road_nodes[1:]):
    sub_gdf = gpd.read_file(file)
    sub_gdf = sub_gdf.loc[sub_gdf["formOfNode"] == "junction"]
    gdf = pd.concat([gdf, sub_gdf], ignore_index=True)
gdf = gdf.to_crs(epsg=4326)
gdf = gdf.reset_index(drop=True)
gdf["geometry"].to_csv(PROCESSED / "junctions.csv", index=True)
