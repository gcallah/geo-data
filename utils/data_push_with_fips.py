import pandas as pd
import os
from pymongo import MongoClient, errors
from rapidfuzz import process, fuzz
import urllib.parse
import re

username = "pb2882"
raw_password = "Chitra@2002"
encoded_password = urllib.parse.quote_plus(raw_password)

MONGO_URI = f"mongodb+srv://{username}:{encoded_password}@cluster0.xjc8gra.mongodb.net/county_data?retryWrites=true&w=majority"
DATABASE_NAME = "county_data"
CSV_FOLDER = "./raw-data"
FIPS_FILE = "./raw-data/state_and_county_fips_master.csv"

client = MongoClient(MONGO_URI, tls=True, serverSelectionTimeoutMS=10000)
db = client[DATABASE_NAME]

# Fallback for not found
manual_fips = {
    ("AK", "Chugach Census Area"): "02063",
    ("AK", "Copper River Census Area"): "02066",
    ("AK", "Kusilvak Census Area"): "02158",
    ("NM", "Doña Ana"): "35013",
    ("NM", "Los Alamos[v]"): "35028",
    ("SD", "Oglala Lakota"): "46113",
    ("VA", "Bedford[z]"): "51019"
}

def normalize(name):
    name = name.lower().strip()
    name = re.sub(r"\[.*?\]", "", name)
    for suffix in [" county", " borough", " census area", " municipality", " city and borough"]:
        name = name.replace(suffix, "")
    name = name.replace("doña", "dona")
    return name.strip()

fips_df = pd.read_csv(FIPS_FILE, dtype=str)
fips_df.columns = [col.strip().lower().replace(" ", "_") for col in fips_df.columns]
fips_df['state_code'] = fips_df['state'].str.upper()
fips_df['county_name'] = fips_df['name'].str.strip()
fips_df['clean_name'] = fips_df['county_name'].apply(normalize)
fips_df['fips_code'] = fips_df['fips'].str.strip()  
def get_fips_code(state_code, county_name):
    if (state_code, county_name) in manual_fips:
        return manual_fips[(state_code, county_name)]

    county_clean = normalize(county_name)
    subset = fips_df[fips_df['state_code'] == state_code]

    if subset.empty:
        return None

    result = process.extractOne(county_clean, subset['clean_name'], scorer=fuzz.WRatio)
    if result and result[1] >= 90:
        match = result[0]
        row = subset[subset['clean_name'] == match]
        if not row.empty:
            return row.iloc[0]['fips_code']  
    return None


inserted_states = set()

def clean_and_upload_csv(file_path):
    try:
        state_code = os.path.basename(file_path).split("_")[0].upper()

        df = pd.read_csv(file_path, sep="\t", header=None, engine='python')
        while df.shape[1] < 5:
            df[df.shape[1]] = None
        df = df.iloc[:, :5]
        df.columns = ["county", "state", "population", "area_sq_miles", "metro_area"]

        state_name = df["state"].dropna().unique()[0]

        df["population"] = (
            df["population"]
            .astype(str)
            .str.replace(",", "")
            .apply(lambda x: int(x) if x.isdigit() else None)
        )
        df["area_sq_miles"] = pd.to_numeric(df["area_sq_miles"], errors="coerce")
        df = df.where(pd.notnull(df), None)
        df["state_code"] = state_code
        df.drop(columns=["state"], inplace=True)

        #match and assign FIPS
        df["fips_code"] = df.apply(lambda row: get_fips_code(state_code, row["county"]), axis=1)
        unmatched_rows = df[df["fips_code"].isna()]
        df = df[df["fips_code"].notna()]

        df["_id"] = df["fips_code"]
        df.drop(columns=["fips_code"], inplace=True)

        records = df.to_dict(orient="records")
        if records:
            try:
                db["counties"].insert_many(records, ordered=False)
                print(f"Inserted {len(records)} counties from {file_path}")
            except errors.BulkWriteError as bwe:
                print(f"Some records already existed for {file_path}")

        if state_code not in inserted_states:
            db["states"].insert_one({
                "state_code": state_code,
                "state_name": state_name
            })
            inserted_states.add(state_code)
            print(f"Added state: {state_code} - {state_name}")

        if not unmatched_rows.empty:
            print(f"Unmatched counties in {file_path}:")
            for row in unmatched_rows.itertuples():
                print(f"   - {row.state_code}: {row.county}")

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def batch_upload(folder):
    for file in os.listdir(folder):
        if file.endswith("_county_pops.csv"):
            clean_and_upload_csv(os.path.join(folder, file))

if __name__ == "__main__":
    batch_upload(CSV_FOLDER)
