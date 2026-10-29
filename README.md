# WMATA Metrorail Data Pipeline

An end-to-end data pipeline for WMATA Metrorail data. It ingests historical ridership archives and real-time train arrival predictions, loads them into DuckDB, transforms them with dbt into tested analytics models, and orchestrates the workflow with Apache Airflow.

> **Status: work in progress.** Phases 1–5 are complete (project setup, Airflow infrastructure, batch ingestion, real-time ingestion, dbt models). Airflow orchestration of the full pipeline is in progress.

---

## What this project demonstrates

- Two different ingestion patterns against real public APIs: **full-refresh batch** for a historical archive, **append-only snapshots** for real-time predictions.
- Layered SQL transformation with dbt: raw → staging (cleaned views) → marts (aggregated tables), with tests and generated documentation.
- Containerized, reproducible local infrastructure via Docker Compose.
- Handling messy real-world data: paginated APIs, mixed-type columns, intermittently unavailable public services.

---

## Architecture

```
DC GIS ArcGIS API ──┐
                    ├──> Python ingestion ──> DuckDB (raw) ──> dbt ──> marts
WMATA Developer API ┘         (scripts/)        (data/)    (metro_analytics/)
                                                              │
                                                              ▼
                                                       Airflow (in progress)
```

Two independent ingestion paths feed two raw tables, which dbt transforms into separate staging and mart models.

---

## Stack

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Python, `requests`, `pandas` | Direct control over pagination and append logic |
| Storage | DuckDB | Embedded, zero-setup, analytical; no cloud account required |
| Transformation | dbt (`dbt-duckdb`) | Version-controlled SQL with tests and lineage docs |
| Orchestration | Apache Airflow (Docker Compose) | Scheduling, retries, and dependency management |
| Containerization | Docker Compose | Reproducible local environment |

---

## Repository structure

```
wmata-metro-pipeline/
├── dags/                      # Airflow DAGs (in progress)
├── data/                      # DuckDB database (gitignored)
├── scripts/
│   ├── ingest_ridership.py    # Batch ingestion: historical ridership
│   └── ingest_realtime.py     # Append ingestion: live predictions
├── metro_analytics/           # dbt project
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/           # stg_ridership, stg_realtime
│       └── marts/             # fct_*, dim_stations
├── docker-compose.yaml
├── requirements.txt
├── .env.example
└── README.md
```

---

## Data sources

| Source | Type | Pattern | Table |
|---|---|---|---|
| DC GIS WMATA Rail Ridership Archive (ArcGIS REST) | Historical snapshots | Full refresh | `raw_ridership` |
| WMATA Developer API — Rail Station Predictions | Live arrival predictions | Append-only | `raw_realtime_predictions` |

**Historical ridership.** The DC GIS service exposes the WMATA Rail Ridership Archive as a queryable ArcGIS layer. The script paginates through it 1,000 rows at a time and rebuilds `raw_ridership` on each run.

**Real-time predictions.** The WMATA Developer API returns upcoming train arrivals for all stations in a single call. Each run stamps every row with a UTC `fetched_at` timestamp and appends to `raw_realtime_predictions`, building a time series of snapshots over successive runs.

---

## Data model

**Raw tables**

| Table | Grain | Load pattern |
|---|---|---|
| `raw_ridership` | station + snapshot time | Full refresh (`DROP` + `CREATE`) |
| `raw_realtime_predictions` | prediction + fetch time | Append (`CREATE IF NOT EXISTS` + `INSERT`) |

**dbt models**

| Model | Layer | Materialization | Grain |
|---|---|---|---|
| `stg_ridership` | staging | view | station + snapshot time |
| `stg_realtime` | staging | view | prediction + fetch time |
| `fct_station_ridership` | mart | table | station + day |
| `fct_prediction_frequency` | mart | table | station + line + hour |
| `dim_stations` | mart | table | station |

---

## Quickstart

### Prerequisites

- Python 3.9+
- Docker Desktop (for Airflow)
- A free WMATA API key from `developer.wmata.com` (Default Tier)

### Setup

```bash
git clone <repo-url>
cd wmata-metro-pipeline

# Python environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Environment variables
cp .env.example .env
# Edit .env and set WMATA_API_KEY
```

### Run the ingestion

```bash
# Historical ridership (batch)
python scripts/ingest_ridership.py

# Real-time predictions (append; run repeatedly to build history)
python scripts/ingest_realtime.py
```

### Run the dbt transformations

```bash
cd metro_analytics
dbt debug
dbt run
dbt test
dbt docs serve   # lineage graph in browser
```

### Airflow (in progress)

```bash
docker compose up -d
# Airflow UI at http://localhost:8080 (airflow / airflow)
```

---

## Known limitations and engineering decisions

- **DC GIS API reliability.** The historical ridership source (`maps2.dcgis.dc.gov`) is a public ArcGIS service that intermittently times out under load. `scripts/ingest_ridership.py` handles this with a 60-second request timeout and up to 3 retries with a 5-second pause between attempts. If all retries fail, the script raises the original timeout error rather than silently returning partial data.

- **Archive layer selection.** The primary archive layer (ID 7) returned zero records when queried. The pipeline targets the active archive layer (ID 9), which contains the same fields and is populated.

- **Full refresh vs. append.** `raw_ridership` is rebuilt on every run because the source returns a complete archive — a new fetch supersedes the old one. `raw_realtime_predictions` is appended because each run is a distinct, immutable observation; full-refreshing would destroy the time series.

- **Mixed-type column.** The `min` field in real-time predictions contains both integers and the strings `"ARR"` / `"BRD"`. Staging uses `TRY_CAST` to convert numeric values to integers (returning `NULL` otherwise) and preserves the string values in a separate `status` column.

- **Local scale.** The pipeline runs on a single machine with DuckDB. Scaling to substantially larger data volumes would mean moving to a cloud warehouse and distributed ingestion.

---

## Roadmap

- [x] Project scaffold and Git setup
- [x] Airflow infrastructure via Docker Compose
- [x] Batch ingestion: historical ridership
- [x] Append ingestion: real-time predictions
- [x] dbt staging and mart models with tests
- [ ] Airflow DAG orchestrating ingestion + `dbt run` / `dbt test`
- [ ] Dashboard (Metabase) on top of the mart tables
