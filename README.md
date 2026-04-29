# snowflake-pipeline

![CI](https://github.com/valtrof/snowflake-pipeline/actions/workflows/ci.yml/badge.svg?branch=main)

End-to-end ELT pipeline ingesting daily weather data from the [Open-Meteo API](https://open-meteo.com/) into **Snowflake**, transformed with **dbt**, orchestrated by **Apache Airflow**, and validated with a full data quality test suite.

## Architecture

```
Open-Meteo API (free, no auth)
    │
    ▼
Python ingestion (incremental — fetches only new dates per city)
    │
    ▼
Snowflake: WEATHER_DB.RAW.RAW_WEATHER_DAILY
    │
    ▼
dbt staging (type casting, null handling)         → STAGING.STG_WEATHER
    │
    ▼
dbt intermediate (derived fields: season,         → INTERMEDIATE.INT_WEATHER_ENRICHED
                  temp range, rain flag)
    │
    ▼
dbt mart (incremental fact table,                 → MARTS.FCT_WEATHER_DAILY
          surrogate key, clustered)
    │
    ▼
Airflow DAG (daily @ 6 AM UTC): ingest → dbt run → dbt test
```

## Stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.11, `snowflake-connector-python`, `requests` |
| Transformation | dbt 1.8 (Snowflake adapter), `dbt_utils` |
| Orchestration | Apache Airflow 2.9 (Docker Compose, LocalExecutor) |
| Warehouse | Snowflake (X-Small warehouse, auto-suspend 60s) |
| CI | GitHub Actions — pytest + dbt compile on every push |
| Testing | pytest (unit, no live connections), dbt schema tests, custom singular test |

## Design Decisions

**Incremental ingestion** — `fetch_weather.py` queries Snowflake for the max loaded date per city before calling the API, avoiding re-fetching historical data on every run. Cold start loads from 2024-01-01.

**Raw layer is all-strings** — the RAW schema stores everything as VARCHAR. Type casting happens exclusively in the dbt staging layer, making schema evolution safe: adding a column to the API response doesn't break the load.

**Incremental dbt mart** — `fct_weather_daily` uses `unique_key=['city', 'observation_date']` so reruns are idempotent. Clustered by `(city, year, month)` for efficient analytical queries.

**Warehouse auto-suspend** — set to 60 seconds to minimize Snowflake credit consumption on the trial account.

**No LangChain / no heavy dependencies in ingestion** — the ingestion layer is a straightforward Python script with minimal dependencies. Keeps the Docker image small and the failure surface narrow.

## Quickstart

### 1. Snowflake setup

Run `setup/snowflake_setup.sql` in your Snowflake worksheet (as ACCOUNTADMIN) to create the warehouse, database, schemas, and role.

### 2. Configure credentials

```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
```

### 3. Run ingestion manually

```bash
pip install -r requirements.txt
python -m ingestion.fetch_weather
```

### 4. Run dbt

```bash
cd dbt
dbt deps
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### 5. Start Airflow (Docker)

```bash
docker-compose up airflow-init
docker-compose up -d
# Open http://localhost:8080 (admin / admin)
# Enable the "weather_pipeline" DAG
```

### 6. Run unit tests

```bash
pytest tests/ -v
```

## Data Quality Tests

| Test | Layer | Type |
|---|---|---|
| `city` not null, accepted values | staging | schema test |
| `observation_date` not null | staging | schema test |
| `weather_id` unique + not null | mart | schema test |
| `season` accepted values | mart | schema test |
| `temp_max_c >= temp_min_c` | staging | custom singular test |
| Source freshness check (warn 25h, error 49h) | raw | freshness |

## Cities Tracked

| City | Latitude | Longitude |
|---|---|---|
| Seattle | 47.6062 | -122.3321 |
| New York | 40.7128 | -74.0060 |
| San Francisco | 37.7749 | -122.4194 |

## Project Structure

```
snowflake-pipeline/
├── ingestion/
│   └── fetch_weather.py        # API → Snowflake incremental loader
├── dbt/
│   ├── models/
│   │   ├── staging/            # Type casting, source definitions
│   │   ├── intermediate/       # Derived fields, enrichment
│   │   └── marts/              # Incremental fact table
│   └── tests/
│       └── assert_temp_max_gte_min.sql
├── dags/
│   └── weather_pipeline.py     # Airflow DAG
├── setup/
│   └── snowflake_setup.sql     # One-time infra provisioning
├── tests/
│   └── test_fetch_weather.py   # Unit tests (no live connections)
├── .github/workflows/ci.yml    # GitHub Actions CI
└── docker-compose.yml          # Airflow local stack
```
