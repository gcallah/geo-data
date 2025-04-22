import pandas as pd
import os
from rapidfuzz import process, fuzz

CSV_FOLDER = "./raw-data"
FIPS_FILE = "./raw-data/state_and_county_fips_master.csv"


fips_df = pd.read_csv(FIPS_FILE, dtype=str)
fips_df.columns = [col.strip().lower().replace(" ", "_") for col in fips_df.columns]
fips_df['state_code'] = fips_df['state'].str.upper()
fips_df['county_name'] = fips_df['name'].str.strip()
fips_df['clean_name'] = fips_df['county_name'].str.lower().replace(
    [" county", " borough", " census area", " municipality", " city and borough"], "", regex=True
)

def normalize(name):
    return name.lower().strip().replace(" county", "").replace(" borough", "").replace(" census area", "").replace(" municipality", "").replace(" city and borough", "")

def fuzzy_match(state_code, county_name):
    county_clean = normalize(county_name)
    subset = fips_df[fips_df['state_code'] == state_code]
    if subset.empty:
        return None
    result = process.extractOne(county_clean, subset['clean_name'], scorer=fuzz.WRatio)
    if result and result[1] >= 90:
        return True
    return False

unmatched = []

for file in os.listdir(CSV_FOLDER):
    if not file.endswith("_county_pops.csv"):
        continue

    state_code = file.split("_")[0].upper()
    file_path = os.path.join(CSV_FOLDER, file)

    df = pd.read_csv(file_path, sep="\t", header=None, engine="python")
    while df.shape[1] < 5:
        df[df.shape[1]] = None
    df = df.iloc[:, :5]
    df.columns = ["county", "state", "population", "area_sq_miles", "metro_area"]

    for county in df["county"].dropna().unique():
        if not fuzzy_match(state_code, county):
            unmatched.append((state_code, county))

print(f" Unmatched counties: {len(unmatched)}")
for state_code, county in unmatched[:15]: 
    print(f"{state_code}: {county}")
