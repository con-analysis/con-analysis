import pandas as pd
import polars as pl
from pathlib import Path
import geopandas as gpd

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'
ANALYSIS = ROOT / 'data' / 'analysis'

df_lookup = pl.read_csv(RAW / "OAs_to_LSOAs_to_MSOAs_to_LEP_to_LAD_(May_2022)_Lookup_in_England.csv", columns=[1, 3])
df_lookup = df_lookup.unique().to_pandas().rename(columns={'LSOA21CD': 'lsoa', 'MSOA21CD': 'msoa'})

gdf_lsoa_geo = gpd.read_file(RAW / 'lsoa.geojson')
# lsoa = gdf['LSOA21CD'].values
gdf_lsoa_geo.rename(columns={'LSOA21CD': 'lsoa'}, inplace=True)
gdf_lsoa_geo.drop(columns=['BNG_E', 'BNG_N', 'LONG', 'LAT', 'GlobalID'], inplace=True)
gdf_lsoa_geo = gdf_lsoa_geo.merge(df_lookup, on='lsoa', how='left')
assert not (gdf_lsoa_geo[gdf_lsoa_geo.isnull().any(axis=1)]['lsoa'].str[0]=='E').any() # No NA in England
gdf_msoa_geo = gdf_lsoa_geo.dissolve(by='msoa')
gdf_msoa_geo = gdf_msoa_geo.reset_index()[['msoa', 'geometry']]


# Should be 6856 MSOA in England
 # https://www.ons.gov.uk/methodology/geography/ukgeographies/censusgeographies/census2021geographies
assert gdf_msoa_geo.shape[0] == 6856

gdf_msoa_geo.to_file(PROCESSED / 'msoa_shape.geojson', driver='GeoJSON')