import pandas as pd
from pathlib import Path
import numpy as np

ROOT = Path.cwd()
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
OUT = ROOT / "out"
CENSUS = RAW / "census"

df = pd.read_csv(RAW / "Stops.csv")
df = df.loc[df["Status"] == "active"]
df = df[["Longitude", "Latitude", "Easting", "Northing"]]
df = df.rename(columns={"Longitude": "lon", "Latitude": "lat"}).reset_index(drop=True)
df.to_csv(PROCESSED / "public_transport_stops.csv", index=True)
