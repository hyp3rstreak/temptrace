from database.insert import insert_weather_data
from database.transform_db import transform_weather_data
from ingest.geocode import geocode_city
from ingest.fetch import get_weather
from ingest.save_to_json import save_to_json
from datetime import datetime
import pandas as pd
import traceback

debug = False
    
def testing():
    city = "Beckley"
    start_date = "2026-01-01"
    end_date = None
    geocoded_city = geocode_city(city)
    print(f"Geocoded city: {geocoded_city}")
    df_loc = pd.DataFrame(data = geocoded_city, index=[0])



    df_hourly, df_daily, real_start_date, real_end_date = get_weather(geocoded_city["latitude"], geocoded_city["longitude"], start_date, end_date)
    
    print(f"Fetching weather data for {geocoded_city['name']} from {real_start_date} to {real_end_date}...")
    # print(f"Hourly dataframe shape: {df_hourly}")

    df_hourly['city'] = geocoded_city['name']
    df_hourly['latitude'] = geocoded_city['latitude']
    df_hourly['longitude'] = geocoded_city['longitude']
    df_hourly['timezone'] = geocoded_city['timezone']

    print("DEBUG == True\nexporting to JSON files ONLY...")
    save_to_json(df_loc, df_hourly, df_daily, geocoded_city, real_start_date, real_end_date)

def main():
    now = datetime.now().strftime("%Y-%m-%d")

    city = input("Enter a city name to fetch weather data - blank for default (NYC): ").strip()
    if city == "":
        city = None
    start_date = input("Enter the start date (YYYY-MM-DD) - blank for today: ")
    if start_date == "":
        start_date = None
    end_date = input("Enter the end date (YYYY-MM-DD) - blank for today: ")
    if end_date == "":
        end_date = None

    geocoded_city = geocode_city(city)
    df_loc = pd.DataFrame(data = geocoded_city, index=[0])

    df_hourly, df_daily, real_start_date, real_end_date = get_weather(geocoded_city["latitude"], geocoded_city["longitude"], start_date, end_date)
    
    print(f"Fetching weather data for {geocoded_city['name']} from {real_start_date} to {real_end_date}...")

    df_hourly['city'] = geocoded_city['name']
    df_hourly['latitude'] = geocoded_city['latitude']
    df_hourly['longitude'] = geocoded_city['longitude']
    df_hourly['timezone'] = geocoded_city['timezone']

    location_row, weather_hourly_rows, weather_daily_rows = transform_weather_data(df_hourly, df_daily, geocoded_city)

    try:
        insert_weather_data(location_row, weather_hourly_rows, weather_daily_rows)
    except Exception as e:
        print(f"Error inserting weather data: {e}")
        traceback.print_exc()

    if save_to_json:
        save_to_json(df_loc, df_hourly, df_daily, geocoded_city, real_start_date, real_end_date)

if __name__ == "__main__":
    if not debug:
        main()
    else:
        testing()