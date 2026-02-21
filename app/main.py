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
    #print(f"Geocoded city: {geocoded_city}")
    print(f"Fetching weather data for {geocoded_city['name']} from {start_date} to {end_date}...")
    result = get_weather(geocoded_city["latitude"], geocoded_city["longitude"], start_date, end_date)

    # Support flexible return shapes from get_weather():
    # - DataFrame
    # - (DataFrame, start_date)
    # - (DataFrame, start_date, end_date)
    if isinstance(result, tuple):
        if len(result) == 3:
            df, real_start_date, real_end_date = result
        elif len(result) == 2:
            df, real_start_date = result
            real_end_date = real_start_date
        elif len(result) == 1:
            df = result[0]
            real_start_date = start_date or now
            real_end_date = end_date or real_start_date
        else:
            df = result[0]
            real_start_date = start_date or now
            real_end_date = end_date or real_start_date
    else:
        df = result
        real_start_date = start_date or now
        real_end_date = end_date or real_start_date
    try:
        print(df.head())
        print(df.tail())
    except Exception:
        print("Fetched data (non-tabular or empty).")

    df.to_json(f"ingest/data/weather_data_{real_start_date}_to_{real_end_date}.json", orient = "records", date_format = "iso", indent=2)

if __name__ == "__main__":
	main()