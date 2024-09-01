from subprocess import call

call(["python", "src/extract/01_extract_lsoa.py"])
call(["python", "src/extract/02_extract_dft_data.py"])
call(["python", "src/extract/03_extract_lsoa_pwc.py"])
call(["python", "src/extract/04_extract_oa_lsoa_21_lookup.py"])

