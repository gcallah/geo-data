import pandas as pd
import os
from pymongo import MongoClient
import urllib.parse

# === MongoDB Setup ===
username = "pb2882"
raw_password = "Chitra@2002"
encoded_password = urllib.parse.quote_plus(raw_password)

MONGO_URI = f"mongodb+srv://{username}:{encoded_password}@cluster0.xjc8gra.mongodb.net/county_data?retryWrites=true&w=majority"
DATABASE_NAME = "county_data"
CSV_FOLDER = "./raw-data"

client = MongoClient(MONGO_URI, tls=True, serverSelectionTimeoutMS=10000)
db = client[DATABASE_NAME]

# Track inserted states
inserted_states = set()

def clean_and_upload_csv(file_path):
    try:
        # Extract state code from file name: e.g., AK_county_pops.csv → AK
        state_code = os.path.basename(file_path).split("_")[0]

        # Read tab-separated file
        df = pd.read_csv(file_path, sep="\t", header=None, engine='python')

        while df.shape[1] < 5:
            df[df.shape[1]] = None
        df = df.iloc[:, :5]
        df.columns = ["county", "state", "population", "area_sq_miles", "metro_area"]

        # Extract the state name (assumes all rows have the same state)
        state_name = df["state"].dropna().unique()[0]

        # Clean population and area
        df["population"] = (
            df["population"]
            .astype(str)
            .str.replace(",", "")
            .apply(lambda x: int(x) if x.isdigit() else None)
        )
        df["area_sq_miles"] = pd.to_numeric(df["area_sq_miles"], errors="coerce")
        df = df.where(pd.notnull(df), None)

        # Replace state name with state code
        df["state_code"] = state_code
        df.drop(columns=["state"], inplace=True)

        # Insert county records into unified "counties" collection
        db["counties"].insert_many(df.to_dict(orient="records"))
        print(f"[✓] Uploaded {len(df)} county records from: {file_path}")

        # Insert unique state record into "states" collection
        if state_code not in inserted_states:
            db["states"].insert_one({
                "state_code": state_code,
                "state_name": state_name
            })
            inserted_states.add(state_code)
            print(f"[✓] Added state: {state_code} - {state_name}")

    except Exception as e:
        print(f"[✗] Error with {file_path}: {e}")

def batch_upload(folder):
    for file in os.listdir(folder):
        if file.endswith("_county_pops.csv"):
            clean_and_upload_csv(os.path.join(folder, file))

if __name__ == "__main__":
    batch_upload(CSV_FOLDER)

