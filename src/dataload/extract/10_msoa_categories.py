import pandas as pd
from pathlib import Path

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'
ANALYSIS = ROOT / 'data' / 'analysis'

p = RAW / (
    "Rural_Urban_Classification_2011_lookup_tables"
    "_for_small_area_geographies.xlsx"
)
df = pd.read_excel(p, sheet_name="MSOA11", header=2)
df = df.iloc[:, [0, 3]]
df.columns = ['msoa', 'rural_urban_category']
df.to_csv(PROCESSED / 'msoa_categories.csv', index=False)