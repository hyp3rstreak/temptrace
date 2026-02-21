import openmeteo_requests

from datetime import datetime
import pandas as pd
import requests_cache
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
## stores responses in a .cache folder expires_after 3600s or 1hr
## prevents unnec API calls - requests_cache
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
## wraps session with retry logic. retry 5 times, wait progessively longer between tries 0.2
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
## creates openmeteo client using the cached + retry session from above
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
## endpoint for forecasts
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
## make the API call
## sends request returns list of responses one per loc
## requests params {} from url
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation: {response.Elevation()} m asl")
print(f"Timezone: {response.Timezone()}{response.TimezoneAbbreviation()}")
print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

# Process current data. The order of variables needs to be the same as requested.
## x = response.::name of dict key with list
current = response.Current()
current_temperature_2m = round(current.Variables(0).Value(),1)
current_apparent_temperature = round(current.Variables(1).Value())
current_is_day = current.Variables(2).Value()
current_precipitation = current.Variables(3).Value()

time = current.Time()

print(f"\nCurrent time: ", datetime.fromtimestamp(time))
print(f"Temp: {current_temperature_2m}")
print(f"Feels like: {current_apparent_temperature}")
if current_is_day < 1:
    print("Night")
else:
    print("Day")
#print(f"Current is_day: {current_is_day}")
print(f"Current precipitation: {current_precipitation}")

# Process daily data. The order of variables needs to be the same as requested.
# ## x = response.::name of dict key with list
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
