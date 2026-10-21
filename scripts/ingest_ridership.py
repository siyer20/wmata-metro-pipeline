import requests
import duckdb
import pandas as pd
import time

#link below points at an ArcGIS Feature Service layer
BASE_URL = (
    "https://maps2.dcgis.dc.gov/dcgis/rest/services/"
    "DCGIS_APPS/WMATA_Rail_Ridership/MapServer/7/query"
)
PAGE_SIZE = 1000
DB_PATH = "data/wmata.duckdb"
TABLE_NAME = "raw_ridership"

def fetch_page(offset):
    params = {
        #1=1 matches every row so returns everything
        "where": "1=1",

        #outlines which columns to pull from
        "outFields": "STATION,CURRENT_IN_TOTAL,CURRENT_OUT_TOTAL,SNAPSHOT_DT",
        
        #don't need spatial geometry data, we only want tabular attributes
        "returnGeometry": "false",
        
        #response format
        "f": "json",
        
        #how far into result set to begin
        "resultOffset": offset,
        
        #how many rows per page (10000)
        "resultRecordCount": PAGE_SIZE, 
    }

    #sends request with outlined parameters
    resp = requests.get(BASE_URL, params=params, timeout=30)

    #'fail fast and loudly' segment to allow for easy problem diagnosis
    resp.raise_for_status()
    data = resp.json()
    if "features" not in data:
        raise RuntimeError(f"Unexpected API response: {data}")
    return [f["attributes"] for f in data["features"]]


def ingest():
    all_rows = []

    #pagination loop
    offset = 0
    while True:
        print(f"Fetching offset {offset}...")
        rows = fetch_page(offset)

        #if api returns an empty page
        if not rows:
            break
        all_rows.extend(rows)

        #if pg has <1000 rows, its the end of the data
        if len(rows) < PAGE_SIZE:
            break

        #advance to next pg
        offset += PAGE_SIZE

        #pausing between reqs for api rate limit
        time.sleep(0.5)  

    if not all_rows:
        raise RuntimeError("No rows fetched from API — check outFields and query parameters")

    #loading data into duckdb
    df = pd.DataFrame(all_rows)
    print(f"Fetched {len(df)} rows")

    con = duckdb.connect(DB_PATH)
    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.execute(f"CREATE TABLE {TABLE_NAME} AS SELECT * FROM df")

    #verification
    count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    print(f"Loaded {count} rows into {DB_PATH}.{TABLE_NAME}")
    con.close()

if __name__ == "__main__":
    ingest()