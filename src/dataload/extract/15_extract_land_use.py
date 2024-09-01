import pandas as pd
from pathlib import Path
import numpy as np

ROOT = Path.cwd()
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
OUT = ROOT / "out"
CENSUS = RAW / "census"

p = RAW / "Live_Tables_-_Land_Use_Stock_2022_-_LSOA.xlsx"
df = pd.read_excel(
    p,
    sheet_name="P404b",
    skiprows=list(range(0, 5)) + list(range(6, 19)),
    usecols=[2, 13, 19, 20, 30, 34, 39, 44, 48],
)
df = df.rename(
    columns={
        "MSOA code": "msoa",
        "Total.2": "commercial_area",
        "Highways and road transport": "road_area",
        "Total.4": "residential_area",
        "Total.7": "developed_area",
        "Total.8": "argicultural_area",
        "Total.9": "forest_area",
        "Total.13": "undeveloped_all_area",
        "Total.15": "total_area",
    }
)
df = df.dropna()
df[["argicultural_area", "forest_area", "commercial_area"]] = (
    df[["argicultural_area", "forest_area", "commercial_area"]]
    .apply(lambda col: col.replace("-", 0))
    .astype(float)
)
df = df.groupby("msoa").sum().reset_index()
df["undeveloped_other_area"] = (
    df["undeveloped_all_area"] - df["argicultural_area"] - df["forest_area"]
)
df["developed_wo_road_res_area"] = (
    df["developed_area"] - df["road_area"] - df["residential_area"]
)
df["developed_wo_res_area"] = df["developed_area"] - df["residential_area"]
df["developed_area_pct"] = df["developed_area"] / df["total_area"]
df["developed_wo_road_res_area_pct"] = (
    df["developed_wo_road_res_area"] / df["total_area"]
)
df["commercial_area_pct"] = df["commercial_area"] / df["total_area"]
df["developed_wo_res_area_pct"] = df["developed_wo_res_area"] / df["total_area"]
df["developed_res_area_pct"] = df["residential_area"] / df["total_area"]
df["undeveloped_area_pct"] = df["undeveloped_all_area"] / df["total_area"]
df["road_pct"] = df["road_area"] / df["total_area"]
df["agricultural_pct"] = df["argicultural_area"] / df["total_area"]
df["forest_pct"] = df["forest_area"] / df["total_area"]
df["undeveloped_other_pct"] = df["undeveloped_other_area"] / df["total_area"]
df.to_csv(PROCESSED / "land_use_pct2.csv", index=False)
