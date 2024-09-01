import pandas as pd
from pathlib import Path

ROOT = Path.cwd()
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"

MINS = 15
SHEETS = {
    "primary": "primary",
    "employment": "emp",
    "secondary": "secondary",
    "further_education": "further_ed",
    "gp": "gp",
    "hospital": "hosp",
    "food_store": "food",
    "town_centre": "town",
}
YEARS = [2019, 2017, 2016, 2015, 2014]
COL_MAP = {
    # which year lsoa is this? think they are 2011
    "005": "lsoa",
    "002": "lad",
    "004": "pop_working_age",
    "101": "travel_time_pt",
    "102": "number_pt_15",  # consider adding in the 15 mins
    "103": "number_pt_30",
    "104": "number_pt_45",
    "105": "number_pt_60",
    "110": "travel_time_cycle",
    "111": "number_cycle_15",
    "112": "number_cycle_30",
    "113": "number_cycle_45",
    "114": "number_cycle_60",
    "119": "travel_time_car",
    "120": "number_car_15",
    "121": "number_car_30",
    "122": "number_car_45",
    "123": "number_car_60",
}
COL_MAP_EMP = {  # employment sheet needs >5000 jobs columns
    "005": "lsoa",
    "002": "lad",
    "004": "pop_working_age",
    "301": "travel_time_pt",
    "302": "number_pt_15",
    "303": "number_pt_30",
    "304": "number_pt_45",
    "305": "number_pt_60",
    "310": "travel_time_cycle",
    "311": "number_cycle_15",
    "312": "number_cycle_30",
    "313": "number_cycle_45",
    "314": "number_cycle_60",
    "319": "travel_time_car",
    "320": "number_car_15",
    "321": "number_car_30",
    "322": "number_car_45",
    "323": "number_car_60",
}

for v in SHEETS.values():
    for year in YEARS:
        print(v, year)
        year_df = pd.read_csv(RAW / f"jts/jts_{v}_{year}.csv")
        year_df = year_df.loc[:, ~year_df.columns.str.contains("^Unnamed")]
        year_df.columns = year_df.columns.str[-3:]

        cm = COL_MAP if v != "emp" else COL_MAP_EMP
        cols = list(cm.keys())

        year_df = year_df[[col for col in cols]]
        year_df = year_df.rename(columns=cm)

        if year_df["pop_working_age"].dtype == "object":
            year_df["pop_working_age"] = (
                year_df["pop_working_age"].str.replace(",", "").astype(float)
            )

        year_df["year"] = year
        year_df["variable"] = v

        if year == 2019:
            sheet_df = year_df
        else:
            # sheet_df = sheet_df.append(year_df)
            sheet_df = pd.concat([sheet_df, year_df], ignore_index=True)

    if v == "primary":
        df = sheet_df
    else:
        df = pd.concat([df, sheet_df], ignore_index=True)

df = df.rename({"variable": "service_type"}, axis=1)
df = df.replace(
    {
        "service_type": {
            "emp": "employment",
            "hosp": "hospital",
            "town": "town_centre",
            "food": "supermarket",
        }
    }
)
df.to_csv(PROCESSED / "travel_times_dft.csv", index=False)

# TODO: Map 2011 - 2021 LSOA - right now I'm not actually using this data but will need to update it

print("02 run successfully")
