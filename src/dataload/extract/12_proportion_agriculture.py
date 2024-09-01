import pandas as pd
from pathlib import Path
import numpy as np

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'
CENSUS = RAW / 'census'

# NB: These will be 2021 MSOAs - which am I using in the main analysis?

p = RAW / "WP015_msoa.csv"
df = pd.read_csv(p)
df = df[['Middle layer Super Output Areas Code', 'Industry (current) (29 categories) Label', 'Count']]
df = df.rename(columns={'Middle layer Super Output Areas Code': 'msoa', 'Industry (current) (29 categories) Label': 'industry', 'Count': 'count'})
df = df.pivot(index='msoa', columns='industry', values='count')
df['total'] = df.sum(axis=1)
df = df.reset_index()
df['proportion_agriculture'] = (df['A Agriculture, forestry and fishing'] + df['B Mining and quarrying']) / df['total']
df = df[['msoa', 'proportion_agriculture', 'total']]
df = df.rename(columns={'total': 'total_employed'})

df.to_csv(PROCESSED / 'proportion_agriculture.csv', index=False)