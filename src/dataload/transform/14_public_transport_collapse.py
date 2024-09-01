import polars as pl
import pandas as pd
from pathlib import Path
import numpy as np
import json
from shapely.geometry import shape, Point
from tqdm import tqdm
import pyproj
from tqdm import tqdm
import warnings

ROOT = Path.cwd()
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
OUT = ROOT / "out"
ANALYSIS = ROOT / "data" / "analysis"

folder = PROCESSED / "public_transport" / "msoa"
files = list(folder.glob("*.csv"))

df_dict = {f.stem: pl.read_csv(f) for f in files}

df = df_dict["locations_1"]
col_order = df.columns
for i in range(2, 12):
    sub_df = df_dict[f"locations_{i}"]
    sub_df = sub_df[col_order]
    df = df.vstack(sub_df)

df_agg = df.group_by("msoa").len()
df_agg = df_agg.sort("msoa")
df_agg.write_csv(PROCESSED / "public_transport_by_msoa.csv")
