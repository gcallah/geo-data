import pandas as pd
from pymongo import MongoClient
import os
import urllib.parse

# Mongo credentials
username = os.getenv("MONGO_USER", "pb2882")
password = urllib.parse.quote_plus(os.getenv("MONGO_PASSWD", "Chitra@2002"))
uri = f"mongodb+srv://{username}:{password}@cluster0.xjc8gra.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(uri, tls=True, serverSelectionTimeoutMS=10000)
db = client["Canada_data"]

CSV_PATH = "./raw-data/canadian_provinces.csv"

def push_provinces():
    df = pd.read_csv(CSV_PATH, dtype=str)
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    df["population"] = pd.to_numeric(df["population"], errors="coerce")
    df = df.where(pd.notnull(df), None)

    records = df.to_dict(orient="records")
    db["provinces"].delete_many({})  # Optional: clear existing data
    db["provinces"].insert_many(records)
    print(f"Inserted {len(records)} Canadian provinces")

if __name__ == "__main__":
    push_provinces()
