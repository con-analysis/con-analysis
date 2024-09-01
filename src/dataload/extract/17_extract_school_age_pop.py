import pandas as pd
from pathlib import Path
import numpy as np

ROOT = Path.cwd()
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
OUT = ROOT / "out"


# Available from here https://www.nomisweb.co.uk/query/construct/submit.asp?forward=yes&menuopt=201&subcomp=
f = RAW / "nomis_2024_08_20_194303.xlsx"
df = pd.read_excel(f, skiprows=7)
df["population_under_12"] = df[
    ["Aged 4 years and under", "Aged 5 to 9 years", "Aged 10 years", "Aged 11 years"]
].sum(axis=1)
df = df.rename(
    columns={
        "2021 super output area - middle layer": "msoa",
        "Total: All usual residents": "population",
    }
)
df = df[["msoa", "population", "population_under_12"]]
df = df.iloc[:7264]
df["propotion_under_12"] = df["population_under_12"] / df["population"]
assert df.msoa.str.contains(":").all()
df["msoa"] = df.msoa.str.split(":", expand=True)[0].str.strip()
df.to_csv(PROCESSED / "population_under_12.csv", index=False)
