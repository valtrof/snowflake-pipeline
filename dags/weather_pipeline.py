"""
Daily weather pipeline DAG.

Schedule: 6 AM UTC daily (yesterday's data is complete and available)
Steps:
  1. ingest_weather   — fetch Open-Meteo API → Snowflake RAW
  2. dbt_run          — run dbt models (staging → intermediate → mart)
  3. dbt_test         — run dbt data quality tests
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _ingest_weather():
    """Import here so Airflow workers don't need the package at parse time."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingestion.fetch_weather import run
    run()


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="weather_pipeline",
    description="Ingest daily weather data from Open-Meteo and transform in Snowflake via dbt",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["weather", "snowflake", "dbt"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_weather",
        python_callable=_ingest_weather,
        doc_md="Fetch new daily weather rows from Open-Meteo API and load into Snowflake RAW layer.",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir . --target prod",
        doc_md="Run all dbt models: staging views → intermediate views → mart incremental table.",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir . --target prod",
        doc_md="Run dbt schema tests and custom singular tests against transformed data.",
    )

    ingest >> dbt_run >> dbt_test
