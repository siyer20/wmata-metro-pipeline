import requests
import duckdb
import pandas as pd

# Layer 6 of the DC GIS WMATA Rail Ridership service: one row per station,
# used as the station reference for dim_stations.
BASE_URL = (
    "https://maps2.dcgis.dc.gov/dcgis/rest/services/"
    "DCGIS_APPS/WMATA_Rail_Ridership/MapServer/6/query"
)
DB_PATH = "data/wmata.duckdb"
TABLE_NAME = "raw_stations"


def fetch_stations(retries=3):
    params = {
        "where": "1=1",
        "outFields": "NAME,WMATA_NAME,LINE,GIS_ID",
        "returnGeometry": "false",
        "f": "json",
        "resultRecordCount": "2000",  # layer has 98 rows; well under any cap
    }
    for attempt in range(retries):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            if "features" not in data:
                raise RuntimeError(f"Unexpected API response: {data}")
            return [f["attributes"] for f in data["features"]]
        except requests.exceptions.Timeout:
            if attempt == retries - 1:
                raise
            print(f"  Timeout, retrying ({attempt + 1}/{retries})...")


def ingest():
    rows = fetch_stations()
    if not rows:
        raise RuntimeError("No rows fetched from API — check outFields and query parameters")

    df = pd.DataFrame(rows)
    print(f"Fetched {len(df)} stations")

    con = duckdb.connect(DB_PATH)
    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.execute(f"CREATE TABLE {TABLE_NAME} AS SELECT * FROM df")

    count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    print(f"Loaded {count} rows into {DB_PATH}.{TABLE_NAME}")
    con.close()


if __name__ == "__main__":
    ingest()