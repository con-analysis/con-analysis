import pandas as pd
from pathlib import Path
import numpy as np
import pyproj
from tqdm import tqdm

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'

# These are already 2021
df_lsoa_pwc = pd.read_csv(RAW / 'lsoa_pwc.csv', ).iloc[:, [1, 3, 4]]
df_lsoa_pwc = df_lsoa_pwc.rename(columns={'LSOA21CD': 'lsoa_21'})

df_lsoa_pwc.to_csv(PROCESSED / 'lsoa_pwc_en.csv', index=False)

print("03 run successfully")