import pandas as pd
from pathlib import Path
import numpy as np

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'
CENSUS = RAW / 'census'

# NB: These will be 2021 MSOAs - which am I using in the main analysis?

p = RAW / "3285411237758082.csv"
df = pd.read_csv(p, header=5)
df = df.iloc[:-5]
df = df.rename(columns={'Total': 'total'})
df['total'] = df['total'].str.replace(',', '').astype(int)
df['msoa'] = df['2021 super output area - middle layer'].str[:9]

assert (df['2021 super output area - middle layer'].str.index(':') == 10).all()
assert (df['2021 super output area - middle layer'].str.index(' ') == 9).all()

df['shared_building'] = df[[
    'In a purpose-built block of flats or tenement', 
    'Part of a converted or shared house, including bedsits',
    'Part of another converted building, for example, former school, church or warehouse',
    'In a commercial building, for example, in an office building, hotel or over a shop'
]].sum(axis=1)
df['proportion_shared_building'] = df['shared_building'] / df['total']
df = df[['msoa', 'total', 'proportion_shared_building']]
df = df.rename(columns={'total': 'total_households'})

df.to_csv(PROCESSED / 'proportion_shared_housing.csv', index=False)