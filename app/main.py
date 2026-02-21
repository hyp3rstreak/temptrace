from database.insert import insert_weather_data
from database.transform_db import transform_weather_data
from ingest.geocode import geocode_city
from ingest.fetch import get_weather
from datetime import datetime

    
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

    df_hourly, df_daily, real_start_date, real_end_date = get_weather(geocoded_city["latitude"], geocoded_city["longitude"], start_date, end_date)
    
    print(f"Fetching weather data for {geocoded_city['name']} from {real_start_date} to {real_end_date}...")

    df_hourly['city'] = geocoded_city['name']
    df_hourly['latitude'] = geocoded_city['latitude']
    df_hourly['longitude'] = geocoded_city['longitude']
    df_hourly['timezone'] = geocoded_city['timezone']

    location_row, weather_hourly_rows, weather_daily_rows = transform_weather_data(df_hourly, df_daily, geocoded_city)

    insert_weather_data(location_row, weather_hourly_rows, weather_daily_rows)
    

    # df_hourly.to_json(f"ingest/data/hourly_{real_start_date}_to_{real_end_date}_{geocoded_city['name']}.json", orient = "records", date_format = "iso", indent=2)
    # print(f"Hourly data saved to ingest/data/hourly_{real_start_date}_to_{real_end_date}_{geocoded_city['name']}.json")
    
if __name__ == "__main__":
	main()