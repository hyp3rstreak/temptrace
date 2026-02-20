import openmeteo_requests

from datetime import datetime
import pandas as pd
import requests_cache
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://api.open-meteo.com/v1/forecast"
params = {
	"latitude": 37.78,
	"longitude": -81.21,
	"daily": ["sunrise", "sunset"],
	"models": "gfs_seamless",
	"current": ["temperature_2m", "apparent_temperature", "is_day", "precipitation"],
	"timezone": "America/New_York",
	"wind_speed_unit": "mph",
	"precipitation_unit": "inch",
	"temperature_unit": "fahrenheit",
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation: {response.Elevation()} m asl")
print(f"Timezone: {response.Timezone()}{response.TimezoneAbbreviation()}")
print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

# Process current data. The order of variables needs to be the same as requested.
current = response.Current()
current_temperature_2m = current.Variables(0).Value()
current_is_day = current.Variables(2).Value()
current_precipitation = current.Variables(3).Value()
current_apparent_temperature = current.Variables(1).Value()
time = current.Time()

print(f"\nCurrent time: ", datetime.fromtimestamp(time))
print(f"Current temperature_2m: {current_temperature_2m}")
print(f"Current app temp: {current_apparent_temperature}")
print(f"Current is_day: {current_is_day}")
print(f"Current precipitation: {current_precipitation}")

# Process daily data. The order of variables needs to be the same as requested.
daily = response.Daily()
daily_sunrise = daily.Variables(0).ValuesInt64AsNumpy()
daily_sunset = daily.Variables(1).ValuesInt64AsNumpy()

daily_data = {"date": pd.date_range(
	start = pd.to_datetime(daily.Time() + response.UtcOffsetSeconds(), unit = "s", utc = True),
	end =  pd.to_datetime(daily.TimeEnd() + response.UtcOffsetSeconds(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = daily.Interval()),
	inclusive = "left"
)}

daily_data["sunrise"] = daily_sunrise
daily_data["sunset"] = daily_sunset

daily_dataframe = pd.DataFrame(data = daily_data)
print("\nDaily data\n", daily_dataframe)
