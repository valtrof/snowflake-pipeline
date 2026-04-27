"""
Fetch daily weather data from Open-Meteo API and load into Snowflake RAW layer.

Incremental: queries Snowflake for the latest loaded date per city,
then fetches only new dates from the API.
"""

import os
import logging
from datetime import date, timedelta

import requests
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CITIES = [
    {"name": "Seattle",       "lat": 47.6062,  "lon": -122.3321},
    {"name": "New York",      "lat": 40.7128,  "lon": -74.0060},
    {"name": "San Francisco", "lat": 37.7749,  "lon": -122.4194},
]

API_BASE = "https://archive-api.open-meteo.com/v1/archive"
DEFAULT_START = "2024-01-01"


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "PIPELINE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "WEATHER_DB"),
        schema="RAW",
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def get_latest_date(cursor, city: str) -> str:
    cursor.execute(
        "SELECT MAX(date) FROM RAW_WEATHER_DAILY WHERE city = %s", (city,)
    )
    row = cursor.fetchone()
    if row and row[0]:
        # resume from the day after the last loaded date
        latest = date.fromisoformat(row[0]) + timedelta(days=1)
        return latest.isoformat()
    return DEFAULT_START


def fetch_weather(city: dict, start_date: str, end_date: str) -> list[dict]:
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max",
        "timezone": "UTC",
    }
    resp = requests.get(API_BASE, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    daily = data.get("daily", {})
    dates = daily.get("time", [])
    if not dates:
        return []

    rows = []
    for i, d in enumerate(dates):
        rows.append({
            "city": city["name"],
            "latitude": str(city["lat"]),
            "longitude": str(city["lon"]),
            "date": d,
            "temp_max_c": str(daily["temperature_2m_max"][i]) if daily.get("temperature_2m_max") else None,
            "temp_min_c": str(daily["temperature_2m_min"][i]) if daily.get("temperature_2m_min") else None,
            "precipitation_mm": str(daily["precipitation_sum"][i]) if daily.get("precipitation_sum") else None,
            "windspeed_max_kmh": str(daily["windspeed_10m_max"][i]) if daily.get("windspeed_10m_max") else None,
        })
    return rows


def load_rows(cursor, rows: list[dict]) -> int:
    if not rows:
        return 0
    cursor.executemany(
        """
        INSERT INTO RAW_WEATHER_DAILY
            (city, latitude, longitude, date, temp_max_c, temp_min_c,
             precipitation_mm, windspeed_max_kmh)
        VALUES (%(city)s, %(latitude)s, %(longitude)s, %(date)s,
                %(temp_max_c)s, %(temp_min_c)s, %(precipitation_mm)s,
                %(windspeed_max_kmh)s)
        """,
        rows,
    )
    return len(rows)


def run(end_date: str | None = None):
    if end_date is None:
        # yesterday — today's data is not yet complete
        end_date = (date.today() - timedelta(days=1)).isoformat()

    conn = get_snowflake_connection()
    cursor = conn.cursor()
    total_loaded = 0

    try:
        for city in CITIES:
            start_date = get_latest_date(cursor, city["name"])
            if start_date > end_date:
                log.info("%s is up to date (latest: %s)", city["name"], start_date)
                continue

            log.info("Fetching %s: %s → %s", city["name"], start_date, end_date)
            rows = fetch_weather(city, start_date, end_date)
            n = load_rows(cursor, rows)
            log.info("Loaded %d rows for %s", n, city["name"])
            total_loaded += n

        conn.commit()
        log.info("Done. Total rows loaded: %d", total_loaded)
    finally:
        cursor.close()
        conn.close()

    return total_loaded


if __name__ == "__main__":
    run()
