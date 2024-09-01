import pandas as pd
from pathlib import Path
import numpy as np

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'

# Prepare OA - LSOA map
df_oa_lookup = pd.read_csv(RAW / 'oa_lsoa_lookup.csv')
df_oa_lookup = df_oa_lookup.rename({'OA21CD': 'oa', 'LSOA21CD': 'lsoa'}, axis=1)
df_oa_lookup = df_oa_lookup[['oa', 'lsoa']]

df_oa_lookup.to_csv(PROCESSED / 'oa_lsoa_21_lookup.csv', index=False)

print("04 run successfully")