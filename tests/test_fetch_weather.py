"""Unit tests for fetch_weather — no real API or Snowflake calls."""

from unittest.mock import MagicMock, patch
from ingestion.fetch_weather import fetch_weather, load_rows, get_latest_date


MOCK_API_RESPONSE = {
    "daily": {
        "time": ["2024-01-01", "2024-01-02"],
        "temperature_2m_max": [10.5, 11.2],
        "temperature_2m_min": [3.1, 4.0],
        "precipitation_sum": [0.0, 2.5],
        "windspeed_10m_max": [15.0, 20.3],
    }
}


def test_fetch_weather_returns_rows():
    city = {"name": "Seattle", "lat": 47.6062, "lon": -122.3321}
    with patch("ingestion.fetch_weather.requests.get") as mock_get:
        mock_get.return_value.json.return_value = MOCK_API_RESPONSE
        mock_get.return_value.raise_for_status = MagicMock()
        rows = fetch_weather(city, "2024-01-01", "2024-01-02")

    assert len(rows) == 2
    assert rows[0]["city"] == "Seattle"
    assert rows[0]["date"] == "2024-01-01"
    assert rows[0]["temp_max_c"] == "10.5"
    assert rows[1]["precipitation_mm"] == "2.5"


def test_fetch_weather_empty_response():
    city = {"name": "Seattle", "lat": 47.6062, "lon": -122.3321}
    with patch("ingestion.fetch_weather.requests.get") as mock_get:
        mock_get.return_value.json.return_value = {"daily": {"time": []}}
        mock_get.return_value.raise_for_status = MagicMock()
        rows = fetch_weather(city, "2024-01-01", "2024-01-01")

    assert rows == []


def test_get_latest_date_with_existing_data():
    cursor = MagicMock()
    cursor.fetchone.return_value = ("2024-03-15",)
    result = get_latest_date(cursor, "Seattle")
    assert result == "2024-03-16"


def test_get_latest_date_no_data():
    cursor = MagicMock()
    cursor.fetchone.return_value = (None,)
    result = get_latest_date(cursor, "Seattle")
    assert result == "2024-01-01"


def test_load_rows_empty():
    cursor = MagicMock()
    n = load_rows(cursor, [])
    assert n == 0
    cursor.executemany.assert_not_called()


def test_load_rows_inserts():
    cursor = MagicMock()
    rows = [{"city": "Seattle", "latitude": "47.6", "longitude": "-122.3",
              "date": "2024-01-01", "temp_max_c": "10.5", "temp_min_c": "3.1",
              "precipitation_mm": "0.0", "windspeed_max_kmh": "15.0"}]
    n = load_rows(cursor, rows)
    assert n == 1
    cursor.executemany.assert_called_once()
