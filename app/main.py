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

from datetime import datetime

from database.insert import insert_weather_data
from database.transform_db import transform_weather_data
from ingest.geocode import geocode_city
from ingest.fetch import get_current_weather, get_forecast_hours, get_forecast_days
from ingest.save_to_json import save_to_json
import pandas as pd
import traceback

# Toggle to run the lightweight `testing()` path instead of the CLI.
debug = False


def run_pipeline(city="Beckley", start_date=None, end_date=None, days_ahead=5, forecast_hours=None, insert_to_db=False, save_json=True, verbose=True):
    """Run the shared geocode/fetch/transform/insert/save workflow.

    Args:
        city (str | None): City name passed to geocoder.
        start_date (str | None): Optional start date (YYYY-MM-DD).
        end_date (str | None): Optional end date (YYYY-MM-DD).
        insert_to_db (bool): When True, inserts transformed rows into DB.
        save_json (bool): When True, exports JSON artifacts.
        verbose (bool): When True, prints full geocoded payload.
    """
    if verbose: 
        print(f"Starting pipeline with city='{city}', start_date='{start_date}', end_date='{end_date}', days_ahead={days_ahead}, forecast_hours='{forecast_hours}', insert_to_db={insert_to_db}, save_json={save_json}, verbose={verbose}")
    # return {} with values for name, latitude, longitude, timezone, elevation, population, country 
    geocoded_city = geocode_city(city)
    if verbose:
        print(f"Geocoded city: {geocoded_city}")
    # create and store a single-row DataFrame with geocoded city info
    df_loc = pd.DataFrame(data=geocoded_city, index=[0])

    # if user specifies forecast_hours, fetch forecast data instead of historical weather
    if forecast_hours is not None:
        df_hourly, df_daily, real_start_date, real_end_date = get_forecast_hours(
            geocoded_city["latitude"], geocoded_city["longitude"], timezone="America/New_York", forecast_hours=forecast_hours
        )
        if verbose:
            print(f"Fetching weather data for {geocoded_city['name']} for the next {forecast_hours} hours.")
    # otherwise, fetch historical weather data for the specified date range (or defaults)
    else:
        df_hourly, df_daily, real_start_date, real_end_date = get_forecast_days(
            geocoded_city["latitude"], geocoded_city["longitude"], start_date, end_date, days_ahead=days_ahead
        )
        if verbose:
            print(f"Fetching weather data for {geocoded_city['name']} from {real_start_date} to {real_end_date}...")


    # Annotate hourly rows with metadata expected by downstream transforms.
    df_hourly = pd.DataFrame(df_hourly).assign(
        city=geocoded_city["name"],
        latitude=geocoded_city["latitude"],
        longitude=geocoded_city["longitude"],
        timezone="America/New_York",
    )
    if verbose:
        print(f"Annotated hourly DataFrame:\n{df_hourly.head()}")

    if forecast_hours is None:
        if insert_to_db:
            location_row, transformed_hourly_rows, transformed_daily_rows = transform_weather_data(
                df_hourly, df_daily, geocoded_city
            )
            try:
                insert_weather_data(location_row, transformed_hourly_rows, transformed_daily_rows)
            except Exception as e:
                print(f"Error inserting weather data: {e}")
                traceback.print_exc()
       
    if save_json:
        if verbose:
            print("Saving JSON output files...")
        save_to_json(df_loc, df_hourly, df_daily, geocoded_city, real_start_date, real_end_date, forecast_hours)

def testing():
    """Run a non-interactive fetch for a fixed city and date.

    This function is intended for local development/debugging. It
    hard-codes `city`, `start_date`, and runs the shared pipeline
    helper without DB insertion.
    """
    print("DEBUG == True\nexporting to JSON files ONLY...")
    run_pipeline(
        forecast_hours=6,
        verbose=True
    )

def get_user_input():
    """Helper to prompt user for city and date range inputs.

    This function centralizes all user input handling and default logic.
    It can be easily extended in the future to include additional prompts
    or validation as needed.
    """
    print("Welcome to the Weather Ingestion Pipeline!\n")
    print("Please provide the following information. Press Enter to use defaults where applicable.")
    print("Note: The default city is 'Beckley'. The default date range is today (start and end).")
    print("For forecasts, the default is the next 12 hours.\n")
    print("1: Next x hours forecast (enter number of hours, e.g. 6)")
    print("2: Next x days forecast (enter number of days, e.g. 5)")
    choice = input("input: ").strip()

    match choice:
        case "1":
            forecast_hours = input("Enter number of hours for forecast (default 12): ").strip()
            if forecast_hours == "":
                forecast_hours = 12
            else:
                try:
                    forecast_hours = int(forecast_hours)
                except ValueError:
                    print("Invalid input for forecast hours. Using default of 12.")
                    forecast_hours = 12
            city_input = input("Enter zip/city to fetch weather data: ").strip()
            if city_input == "":
                    city_input = "Beckley"
            else:
                try:
                    city_input = str(city_input)
                except ValueError:
                    print("Invalid input for city. Using default.")
                    city_input = "Beckley"
            start_date = None
            end_date = None
            days_ahead = None

        case "2":
            city_input = input("Enter zip/city to fetch weather data: ").strip()
            if city_input == "":    
                city_input = "Beckley"
            else:
                try:
                    city_input = str(city_input)
                except ValueError:
                    print("Invalid input for city. Using default of Beckley.")
                    city_input = "Beckley"
            start_date = input("Enter the start date (YYYY-MM-DD) - blank for today: ")
            if start_date == "":
                start_date = None
            days_ahead = input("Enter the number of days ahead to fetch (default 5): ")
            if days_ahead == "":
                days_ahead = 5
            forecast_hours = None
            end_date = None

    insert_input = input("Insert fetched data into database? (y/n, default n): ").strip().lower()
    if insert_input == "y":
        insert_to_db = True
    else:
        insert_to_db = False
    verbose_input = input("Print verbose output? (y/n, default y): ").strip().lower()
    if verbose_input == "n":
        verbose = False
    else:
        verbose = True
    json_input = input("Save JSON output files? (y/n, default y): ").strip().lower()
    if json_input == "y":
        save_json = True
    else:
        save_json = True

    return city_input, start_date, end_date, days_ahead, forecast_hours, insert_to_db, save_json, verbose

def main():
    """Interactive CLI entry point.

    Prompts the user for a city and optional start/end dates. Defaults
    are used when the inputs are blank. The function orchestrates the
    same fetch/transform/insert flow used in `testing()` and writes JSON
    output at the end.
    """
    city_input, start_date, end_date, days_ahead, forecast_hours, insert_to_db, save_json, verbose = get_user_input()

    run_pipeline(city=city_input, start_date=start_date, end_date=end_date, days_ahead=days_ahead, 
                    forecast_hours=forecast_hours, insert_to_db=insert_to_db, save_json=save_json, verbose=verbose)


if __name__ == "__main__":
    if not debug:
        main()
    else:
        testing()
