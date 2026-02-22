"""
CLI entrypoint and test helper for the weather ingestion pipeline.

This module orchestrates the high-level flow:
 - Geocode a user-provided city name
 - Fetch hourly and daily weather DataFrames via `ingest.fetch.get_weather`
 - Annotate the hourly DataFrame with location metadata
 - Transform DataFrames into DB-ready tuples via `database.transform_db`
 - Insert/upsert the data into Postgres via `database.insert`
 - Optionally persist JSON outputs using `ingest.save_to_json`

There are two entry paths:
 - `main()`: interactive CLI used in normal runs
 - `testing()`: a hard-coded run useful for quick local debugging when
   `debug` is set to True.
"""

from database.insert import insert_weather_data
from database.transform_db import transform_weather_data
from ingest.geocode import geocode_city
from ingest.fetch import get_weather
from ingest.save_to_json import save_to_json
import pandas as pd
import traceback

# Toggle to run the lightweight `testing()` path instead of the CLI.
debug = False


def run_pipeline(city, start_date=None, end_date=None, insert_to_db=True, save_json=True, verbose=False):
    """Run the shared geocode/fetch/transform/insert/save workflow.

    Args:
        city (str | None): City name passed to geocoder.
        start_date (str | None): Optional start date (YYYY-MM-DD).
        end_date (str | None): Optional end date (YYYY-MM-DD).
        insert_to_db (bool): When True, inserts transformed rows into DB.
        save_json (bool): When True, exports JSON artifacts.
        verbose (bool): When True, prints full geocoded payload.
    """
    geocoded_city = geocode_city(city)
    if verbose:
        print(f"Geocoded city: {geocoded_city}")
    df_loc = pd.DataFrame(data=geocoded_city, index=[0])

    df_hourly, df_daily, real_start_date, real_end_date = get_weather(
        geocoded_city["latitude"], geocoded_city["longitude"], start_date, end_date
    )

    print(f"Fetching weather data for {geocoded_city['name']} from {real_start_date} to {real_end_date}...")

    # Annotate hourly rows with metadata expected by downstream transforms.
    df_hourly["city"] = geocoded_city["name"]
    df_hourly["latitude"] = geocoded_city["latitude"]
    df_hourly["longitude"] = geocoded_city["longitude"]
    df_hourly["timezone"] = geocoded_city["timezone"]

    if insert_to_db:
        location_row, weather_hourly_rows, weather_daily_rows = transform_weather_data(
            df_hourly, df_daily, geocoded_city
        )
        try:
            insert_weather_data(location_row, weather_hourly_rows, weather_daily_rows)
        except Exception as e:
            print(f"Error inserting weather data: {e}")
            traceback.print_exc()

    if save_json:
        save_to_json(df_loc, df_hourly, df_daily, geocoded_city, real_start_date, real_end_date)


def testing():
    """Run a non-interactive fetch for a fixed city and date.

    This function is intended for local development/debugging. It
    hard-codes `city`, `start_date`, and runs the shared pipeline
    helper without DB insertion.
    """
    print("DEBUG == True\nexporting to JSON files ONLY...")
    run_pipeline(
        city="Beckley",
        start_date="2026-01-01",
        end_date=None,
        insert_to_db=False,
        save_json=True,
        verbose=True,
    )


def main():
    """Interactive CLI entry point.

    Prompts the user for a city and optional start/end dates. Defaults
    are used when the inputs are blank. The function orchestrates the
    same fetch/transform/insert flow used in `testing()` and writes JSON
    output at the end.
    """
    # Prompt user for city/date range; blank values indicate defaults.
    city = input("Enter a city name to fetch weather data - blank for default (NYC): ").strip()
    if city == "":
        city = None
    start_date = input("Enter the start date (YYYY-MM-DD) - blank for today: ")
    if start_date == "":
        start_date = None
    end_date = input("Enter the end date (YYYY-MM-DD) - blank for today: ")
    if end_date == "":
        end_date = None

    run_pipeline(city=city, start_date=start_date, end_date=end_date, insert_to_db=True, save_json=True)


if __name__ == "__main__":
    if not debug:
        main()
    else:
        testing()
