import pandas as pd
import polars as pl
from pathlib import Path
import geopandas as gpd
import numpy as np

ROOT = Path.cwd()
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
OUT = ROOT / "out"
ANALYSIS = ROOT / "data" / "analysis"


# WHAT LEVEL IS THERE ABOVE MSOA?

# Read in LSOA to MSOA lookup
df_lookup = pl.read_csv(
    RAW / "OAs_to_LSOAs_to_MSOAs_to_LEP_to_LAD_(May_2022)_Lookup_in_England.csv",
    columns=[1, 3, 9],
)
# df_lookup = pl.read_csv(RAW / "OAs_to_LSOAs_to_MSOAs_to_LEP_to_LAD_(May_2022)_Lookup_in_England.csv")
df_lookup = (
    df_lookup.unique()
    .to_pandas()
    .rename(columns={"LSOA21CD": "lsoa", "MSOA21CD": "msoa", "LAD22CD": "lad"})
)
df_lookup.to_csv(PROCESSED / "msoa_lookup.csv", index=False)

# Map place type LSOA to MSOA###
df_census = pd.read_csv(PROCESSED / "census_stats.csv")
df_census = df_census.merge(df_lookup, on="lsoa", how="left").rename(
    columns={"msoa21cd": "msoa"}
)
census_dummies = pd.get_dummies(df_census["rural_urban_category"], prefix="cat").astype(
    "int"
)
df_census = pd.concat([df_census, census_dummies], axis=1).drop(
    columns=["rural_urban_category"]
)
# df_census_msoa = df_census.groupby('msoa').sum().reset_index().drop(columns=['lsoa'])

aggregation = {
    (col): (
        "sum" if col.startswith("cat_") or col in ["population", "area"] else "first"
    )
    for col in [col1 for col1 in df_census.columns if col1 != "msoa"]
}
df_census_msoa = (
    df_census.groupby("msoa")
    .agg(aggregation)
    .reset_index()
    .drop(columns=["lsoa", "pop_density"])
)
df_census_msoa["rural_urban_category"] = (
    df_census_msoa[[col for col in df_census_msoa.columns if col.startswith("cat_")]]
    .idxmax(axis=1)
    .str.split("_")
    .str[1]
)
df_census_msoa = df_census_msoa[
    ["msoa", "lad", "population", "area", "rural_urban_category"]
]

# Instead add in MSOA cat from ONS
df_msoa_cat = pd.read_csv(PROCESSED / "msoa_categories.csv")
# df_census_msoa = df_census_msoa.drop(columns=['rural_urban_category'])
df_census_msoa = df_census_msoa.merge(
    df_msoa_cat, on="msoa", how="left"
)  # why are some missing?
df_census_msoa["rural_urban_category_y"] = np.where(
    df_census_msoa["rural_urban_category_y"].isnull(),
    df_census_msoa["rural_urban_category_x"],
    df_census_msoa["rural_urban_category_y"],
)
df_census_msoa = df_census_msoa.drop(columns=["rural_urban_category_x"]).rename(
    columns={"rural_urban_category_y": "rural_urban_category"}
)

# Read in # vars
df_per_lsoa = pd.read_csv(
    "/Users/toby/Dev/connectivity-analysis/data/analysis/services_by_lsoa.csv"
)
df_per_lsoa = df_per_lsoa.loc[
    df_per_lsoa["lsoa"].str[0] == "E"
]  # Only need English locations

# Check that services match the values in the original dataset - they do
SERVICE_COLS = [
    "emp",
    "food",
    "further_ed",
    "gp",
    "hosp",
    "primary",
    "secondary",
    "town_centre",
]
df_per_lsoa[SERVICE_COLS].sum().equals(
    pd.Series([843, 15147.0, 2198, 6866, 219, 16948, 3128, 1211], index=SERVICE_COLS)
)
df_per_lsoa = df_per_lsoa.drop(columns=["lat", "lon"])
df_per_lsoa = df_per_lsoa.merge(df_lookup, on="lsoa", how="left").rename(
    columns={"msoa21cd": "msoa"}
)
df_per_msoa = (
    df_per_lsoa.drop(columns=["lsoa"])
    .groupby("msoa")
    .agg(
        {
            "emp": "sum",
            "food": "sum",
            "further_ed": "sum",
            "gp": "sum",
            "hosp": "sum",
            "primary": "sum",
            "secondary": "sum",
            "town_centre": "sum",
        }
    )
    .reset_index()
)

# Read in MSOA geojson
gdf_msoa_geo = gpd.read_file(PROCESSED / "msoa_shape.geojson")
assert gdf_msoa_geo.shape[0] == 6856

# Add in services
gdf_msoa_geo = gdf_msoa_geo.merge(df_per_msoa, on="msoa", how="left")
gdf_msoa_geo = gdf_msoa_geo.fillna(0)
gdf_msoa_geo.info()

# Add in census data
gdf_msoa_geo = gdf_msoa_geo.merge(df_census_msoa, on="msoa", how="left")
# gdf_msoa_geo.info()

# add in Region
df_region = pd.read_csv(
    RAW / "Local_Authority_District_to_Region_(December_2022)_Lookup_in_England.csv"
)
df_region = (
    df_region[["LAD22CD", "RGN22CD"]]
    .rename(columns={"LAD22CD": "lad", "RGN22CD": "region"})
    .drop_duplicates()
)
gdf_msoa_geo = gdf_msoa_geo.merge(df_region, on="lad", how="left")

# Add shared housing and agriculture
df_agriculture = pd.read_csv(PROCESSED / "proportion_agriculture.csv", usecols=[0, 1])
df_shared_housing = pd.read_csv(
    PROCESSED / "proportion_shared_housing.csv", usecols=[0, 2]
)
gdf_msoa_geo = gdf_msoa_geo.merge(df_agriculture, on="msoa", how="left")
gdf_msoa_geo = gdf_msoa_geo.merge(df_shared_housing, on="msoa", how="left")

# Add distances to Town centre
df_town = pd.read_csv(ANALYSIS / "nearest_town_centre_by_LSOA.csv").rename(
    columns={"lsoa_origin": "lsoa"}
)
df_town = df_town.merge(df_lookup, on="lsoa", how="left")
df_town = df_town.groupby("msoa").agg({"distance_km": "min"}).reset_index()
gdf_msoa_geo = gdf_msoa_geo.merge(df_town, on="msoa", how="left")
gdf_msoa_geo = gdf_msoa_geo.dropna(subset=["distance_km"])

# Add distances to rail/road
# previously used distances_to_rail_road2
# df_rail_road = pd.read_csv(PROCESSED / "distances_to_rail_road3.csv")
df_rail_road = pd.read_csv(PROCESSED / "distances_to_rail_road2.csv")
gdf_msoa_geo = gdf_msoa_geo.merge(df_rail_road, on="msoa", how="left")
gdf_msoa_geo["population_density"] = gdf_msoa_geo["population"] / gdf_msoa_geo["area"]

# Add land use
df_land_use = pd.read_csv(PROCESSED / "land_use_pct2.csv")
gdf_msoa_geo = gdf_msoa_geo.merge(df_land_use, on="msoa", how="left")
gdf_msoa_geo["rural_urban_category"] = (
    gdf_msoa_geo["rural_urban_category"].str.split().str.join(" ")
)

# Add public transport density
df_public_transport = pd.read_csv(PROCESSED / "public_transport_by_msoa.csv")
df_public_transport = df_public_transport.dropna(subset=["msoa"])
df_public_transport = df_public_transport.rename(
    columns={"len": "n_public_transport_stops"}
)
gdf_msoa_geo = gdf_msoa_geo.merge(df_public_transport, on="msoa", how="left")
gdf_msoa_geo["n_public_transport_stops"] = gdf_msoa_geo[
    "n_public_transport_stops"
].fillna(0)
gdf_msoa_geo["public_transport_density"] = (
    gdf_msoa_geo["n_public_transport_stops"] / gdf_msoa_geo["area"]
)

# Add school capacity
df_schools = pd.read_csv(PROCESSED / "school_capacity.csv")
msoa_without_schools = gdf_msoa_geo.shape[0] - df_schools.shape[0]
# cool so seems very few will be missing
gdf_msoa_geo[gdf_msoa_geo.primary == 0].shape[0]

# Add school age population
df_school_age = pd.read_csv(PROCESSED / "population_under_12.csv")
df_school_age.msoa = df_school_age.msoa.str.strip(" ")
pre = gdf_msoa_geo.shape[0]
gdf_msoa_geo = gdf_msoa_geo.merge(
    df_school_age[["msoa", "population_under_12"]], on="msoa", how="left"
)
assert gdf_msoa_geo[gdf_msoa_geo.population_under_12.isnull()].shape[0] == 0
assert pre == gdf_msoa_geo.shape[0]

gdf_msoa_geo = gdf_msoa_geo.merge(df_schools, on="msoa", how="left")
gdf_msoa_geo[["school_capacity", "number_of_pupils"]].info()
gdf_msoa_geo[["school_capacity", "number_of_pupils"]] = gdf_msoa_geo[
    ["school_capacity", "number_of_pupils"]
].fillna(0)

gdf_msoa_geo.to_file(PROCESSED / "predictors_msoa8.geojson", driver="GeoJSON")
