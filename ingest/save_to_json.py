import json

def save_to_json(df_loc, df_hourly, df_daily, geocoded_city, real_start_date, real_end_date):
    city = geocoded_city['name']
    
    try:
        df_loc.to_json(f"ingest/data/geocoded_city_{city.replace(' ', '_')}.json", orient = "records", date_format = "iso", indent=2)
        df_hourly.to_json(f"ingest/data/hourly_{real_start_date}_to_{real_end_date}_{geocoded_city['name']}.json", orient = "records", date_format = "iso", indent=2)
        df_daily.to_json(f"ingest/data/daily_{real_start_date}_to_{real_end_date}_{geocoded_city['name']}.json", orient = "records", date_format = "iso", indent=2)
        print(f"Geocoded city data saved to ingest/data/geocoded_city_{city.replace(' ', '_')}.json")
        print(f"Hourly data saved to ingest/data/hourly_{real_start_date}_to_{real_end_date}_{geocoded_city['name']}.json")
        print(f"Daily data saved to ingest/data/daily_{real_start_date}_to_{real_end_date}_{geocoded_city['name']}.json")
    except Exception as e:
        print(f"Error saving to JSON: {e}")