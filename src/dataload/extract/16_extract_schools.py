import pandas as pd
from pathlib import Path
import numpy as np

ROOT = Path.cwd()
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
OUT = ROOT / "out"


# Available from here https://get-information-schools.service.gov.uk/Establishments/Search?tok=0qfuVY6i
f = RAW / "primary_school_data.xlsx"
df = pd.read_excel(f)

# df.head()
# df[['SchoolCapacity', "NumberOfPupils"]].info()
# df[['SchoolCapacity', "NumberOfPupils"]].describe()
# df = df.dropna(subset=["NumberOfPupils"])
# df.describe(include="all")
# df['ReasonEstablishmentClosed (name)'].value_counts()
# df
# df.columns
df = df.groupby("MSOA (code)")[['SchoolCapacity', "NumberOfPupils"]].sum().reset_index()
# df.head()
df = df.rename(columns={"MSOA (code)": "msoa", "SchoolCapacity": "school_capacity", "NumberOfPupils": "number_of_pupils"})
df = df.loc[df.msoa != "999999999"]
df.to_csv(PROCESSED / "school_capacity.csv", index=False)

