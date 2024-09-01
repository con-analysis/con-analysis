import pandas as pd
from pathlib import Path
import numpy as np
import geopandas as gpd

from libpysal.weights import Queen, Rook, KNN

ROOT = Path.cwd()
RAW = ROOT / 'data' / 'raw'
PROCESSED = ROOT / 'data' / 'processed'
OUT = ROOT / 'out'
ANALYSIS = ROOT / 'data' / 'analysis'

# First use Rook connectivity - could adjust this to Queen
gdf = gpd.read_file(RAW / 'lsoa.geojson')
w_rook = Rook.from_dataframe(gdf)
wf, ids = w_rook.full()
wf.tofile(ANALYSIS / 'neighborhood_matrix_rook.csv', sep=',')