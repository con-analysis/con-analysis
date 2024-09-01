from pathlib import Path
import polars as pl

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'
ANALYSIS = ROOT / 'data' / 'analysis'

DIST_MAX_KM = 50

df_od = pl.read_csv(ANALYSIS / f"distances_between_lsoa_{DIST_MAX_KM}.csv")
df_services = pl.read_csv(ANALYSIS / "services_by_lsoa.csv")
df_services = df_services.drop(['lat', 'lon'])

df_od =  df_od.join(df_services, left_on='lsoa_destination', right_on='lsoa', how='left')

# create count dataset
df_od_collapsed = df_od.group_by('lsoa_origin').sum()
df_od_collapsed = df_od_collapsed.drop(['lsoa_destination', 'distance_km'])
df_od_collapsed = df_od_collapsed.rename({'lsoa_origin': 'lsoa'})

df_od_collapsed.write_csv(ANALYSIS / f"services_within_{DIST_MAX_KM}km.csv")


# Create dataset with min distance to service
# what if there's none within 50km? some of these will get dropped, which is not ideal...
df_od_min = df_od[['lsoa_origin', 'distance_km', 'town_centre']].filter( (pl.col('town_centre')!= 0))
df_od_min = df_od_min[['lsoa_origin', 'distance_km']].group_by('lsoa_origin').min()
df_od_min.write_csv(ANALYSIS / f"nearest_town_centre_by_LSOA_{DIST_MAX_KM}.csv")

