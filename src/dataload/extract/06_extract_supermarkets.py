import pandas as pd
from pathlib import Path
import numpy as np

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'

# data here: https://geolytix.com/blog/supermarket-retail-points/
# does NIC report consider services at all? is there any reports which make use of 
# number of supermarkets etc?
PATH = RAW / 'geolytix_retailpoints_v31_202403.xlsx'

df = pd.read_excel(PATH)
df = df[['fascia', 'long_wgs', 'lat_wgs', 'town', 'county']]
df.columns = ['fascia', 'lon', 'lat', 'town', 'county']
df.to_csv(PROCESSED / 'supermarkets_lat_lon.csv', index=False)