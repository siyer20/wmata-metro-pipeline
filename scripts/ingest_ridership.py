import requests
import duckdb
import pandas as pd
import time

#link below points at an ArcGIS Feature Service layer
BASE_URL = (
    "https://maps2.dcgis.dc.gov/dcgis/rest/services/"
    "DCGIS_APPS/WMATA_Rail_Ridership/MapServer/9/query"
)

#page_size set to server's MaxRecordCount
PAGE_SIZE = 1000
DB_PATH = "data/wmata.duckdb"
TABLE_NAME = "raw_ridership"

def fetch_page(offset, retries=3):
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

        # Deterministic ordering so offsets don't overlap/skip across requests.
        "orderByFields": "OBJECTID",
    }

    #retry up to 3 times
    for attempt in range(retries):
        try: #catches only timeout errors
            #sends request with outlined parameters
            resp = requests.get(BASE_URL, params=params, timeout=60) #timeout changed from 30->60

            #'fail fast and loudly' segment to allow for easy problem diagnosis
            resp.raise_for_status()
            data = resp.json()
            if "features" not in data:
                raise RuntimeError(f"Unexpected API response: {data}")
            return [f["attributes"] for f in data["features"]]
        except requests.exceptions.Timeout:
            #on final attempt, raise timeout error instead of failing quietly
            if attempt == retries - 1:
                raise
            #logs each retry
            print(f"  Timeout on offset {offset}, retrying ({attempt + 1}/{retries})...")

            #wait 5 seconds between tries to give server time to catch up
            time.sleep(5)



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

        #advance by rows received
        offset += len(rows)
        print(f"  got {len(rows)} rows, total so far {len(all_rows)}")

        #pausing between reqs for api rate limit
        time.sleep(0.5)

    #guard against loading empty databases
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