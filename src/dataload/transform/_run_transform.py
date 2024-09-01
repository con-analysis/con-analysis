from subprocess import call

for path in [
    # "src/transform/01_process_lsoa_pwc.py",
    "src/transform/02_process_accessibility_data.py",
    "src/transform/03_process_commute_data.py",
    "src/transform/04_process_census_data.py",
    "src/transform/05_process_ttm.py",
    # "src/transform/06_geodistance_lsoa.py",
    # "src/transform/07_create_accessibility_index.py",
    # "src/transform/08_combine_destination_data.py",
    # "src/transform/09_combine_predictors.py",
]:
    call(["python", path])

