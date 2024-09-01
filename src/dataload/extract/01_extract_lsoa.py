import pandas as pd
from pathlib import Path
import numpy as np

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'
CENSUS = RAW / 'census'

path = RAW / 'lsoa_lookup_2011_2021.csv'

df = pd.read_csv(path)
assert len(df.loc[df.CHGIND == 'X']) == 10
df = df.loc[df.CHGIND != 'X'] # Drop the 10 irregularly changed LSOAs

# TODO: Should I also filter to include only England here?
# Perhaps not, as I may want to consider Wales and Scotland as destinations

df = df[['LSOA11CD', 'LSOA21CD', 'CHGIND']]
df.columns = ['lsoa_11', 'lsoa_21', 'CHGIND']
df.to_csv(PROCESSED / 'lsoa_lookup.csv', index=False)

print("01 run successfully")
