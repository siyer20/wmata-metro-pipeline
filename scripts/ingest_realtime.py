import os
import requests
import duckdb
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load .env from the repo root, regardless of the current working directory
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

API_KEY = os.environ["WMATA_API_KEY"]

#GetPredictionAll returns predictions for every station in one response
BASE_URL = "https://api.wmata.com/StationPrediction.svc/json/GetPrediction/All"
DB_PATH = "data/wmata.duckdb"
TABLE_NAME = "raw_realtime_predictions"


def fetch_predictions():

    #api call
    headers = {"api_key": API_KEY}
    resp = requests.get(BASE_URL, headers=headers, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    if "Trains" not in data:
        raise RuntimeError(f"Unexpected response: {data}")
    return data["Trains"]

def ingest():
    trains = fetch_predictions()
    if not trains:
        print("No predictions returned (system may be closed).")
        return

    #get timestamp
    fetched_at = datetime.now(timezone.utc)

    #reading data into table
    rows = []
    for t in trains:
        rows.append({
            "fetched_at": fetched_at,
            "line": t.get("Line"),
            "destination_name": t.get("DestinationName"),
            "min": t.get("Min"),
            "car_count": t.get("Car"),
            "location_code": t.get("LocationCode"),
            "location_name": t.get("LocationName"),
        })

    df = pd.DataFrame(rows)
    print(f"Fetched {len(df)} predictions at {fetched_at}")

    con = duckdb.connect(DB_PATH)

    #creates empty table on first run, does nothing if table already exists
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} AS
        SELECT * FROM df LIMIT 0
    """)

    #appends current batch of rows onto table
    con.execute(f"INSERT INTO {TABLE_NAME} SELECT * FROM df")
    count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    print(f"Table now has {count} total rows")
    con.close()

if __name__ == "__main__":
    ingest()