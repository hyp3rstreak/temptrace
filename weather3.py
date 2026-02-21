import openmeteo_requests

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
	"longitude": -81.19,
	"hourly": ["temperature_2m", "apparent_temperature", "precipitation_probability", "relative_humidity_2m", "dew_point_2m", "precipitation", "rain", "showers", "snowfall", "snow_depth", "cloud_cover", "visibility", "weather_code", "sunshine_duration", "is_day", "uv_index_clear_sky"],
	"current": ["temperature_2m", "relative_humidity_2m", "apparent_temperature", "weather_code", "is_day", "cloud_cover", "precipitation", "rain", "showers", "snowfall", "wind_speed_10m", "wind_gusts_10m", "wind_direction_10m", "pressure_msl", "surface_pressure"],
	"timezone": "America/New_York",
	"wind_speed_unit": "mph",
	"temperature_unit": "fahrenheit",
	"precipitation_unit": "inch",
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
current_relative_humidity_2m = current.Variables(1).Value()
current_apparent_temperature = current.Variables(2).Value()
current_weather_code = current.Variables(3).Value()
current_is_day = current.Variables(4).Value()
current_cloud_cover = current.Variables(5).Value()
current_precipitation = current.Variables(6).Value()
current_rain = current.Variables(7).Value()
current_showers = current.Variables(8).Value()
current_snowfall = current.Variables(9).Value()
current_wind_speed_10m = current.Variables(10).Value()
current_wind_gusts_10m = current.Variables(11).Value()
current_wind_direction_10m = current.Variables(12).Value()
current_pressure_msl = current.Variables(13).Value()
current_surface_pressure = current.Variables(14).Value()

print(f"\nCurrent time: {current.Time()}")
print(f"Current temperature_2m: {current_temperature_2m}")
print(f"Current relative_humidity_2m: {current_relative_humidity_2m}")
print(f"Current apparent_temperature: {current_apparent_temperature}")
print(f"Current weather_code: {current_weather_code}")
print(f"Current is_day: {current_is_day}")
print(f"Current cloud_cover: {current_cloud_cover}")
print(f"Current precipitation: {current_precipitation}")
print(f"Current rain: {current_rain}")
print(f"Current showers: {current_showers}")
print(f"Current snowfall: {current_snowfall}")
print(f"Current wind_speed_10m: {current_wind_speed_10m}")
print(f"Current wind_gusts_10m: {current_wind_gusts_10m}")
print(f"Current wind_direction_10m: {current_wind_direction_10m}")
print(f"Current pressure_msl: {current_pressure_msl}")
print(f"Current surface_pressure: {current_surface_pressure}")

# Process hourly data. The order of variables needs to be the same as requested.
hourly = response.Hourly()
hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
hourly_apparent_temperature = hourly.Variables(1).ValuesAsNumpy()
hourly_precipitation_probability = hourly.Variables(2).ValuesAsNumpy()
hourly_relative_humidity_2m = hourly.Variables(3).ValuesAsNumpy()
hourly_dew_point_2m = hourly.Variables(4).ValuesAsNumpy()
hourly_precipitation = hourly.Variables(5).ValuesAsNumpy()
hourly_rain = hourly.Variables(6).ValuesAsNumpy()
hourly_showers = hourly.Variables(7).ValuesAsNumpy()
hourly_snowfall = hourly.Variables(8).ValuesAsNumpy()
hourly_snow_depth = hourly.Variables(9).ValuesAsNumpy()
hourly_cloud_cover = hourly.Variables(10).ValuesAsNumpy()
hourly_visibility = hourly.Variables(11).ValuesAsNumpy()
hourly_weather_code = hourly.Variables(12).ValuesAsNumpy()
hourly_sunshine_duration = hourly.Variables(13).ValuesAsNumpy()
hourly_is_day = hourly.Variables(14).ValuesAsNumpy()
hourly_uv_index_clear_sky = hourly.Variables(15).ValuesAsNumpy()

hourly_data = {"date": pd.date_range(
	start = pd.to_datetime(hourly.Time() + response.UtcOffsetSeconds(), unit = "s", utc = True),
	end =  pd.to_datetime(hourly.TimeEnd() + response.UtcOffsetSeconds(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = hourly.Interval()),
	inclusive = "left"
)}

hourly_data["temperature_2m"] = hourly_temperature_2m
hourly_data["apparent_temperature"] = hourly_apparent_temperature
hourly_data["precipitation_probability"] = hourly_precipitation_probability
hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
hourly_data["dew_point_2m"] = hourly_dew_point_2m
hourly_data["precipitation"] = hourly_precipitation
hourly_data["rain"] = hourly_rain
hourly_data["showers"] = hourly_showers
hourly_data["snowfall"] = hourly_snowfall
hourly_data["snow_depth"] = hourly_snow_depth
hourly_data["cloud_cover"] = hourly_cloud_cover
hourly_data["visibility"] = hourly_visibility
hourly_data["weather_code"] = hourly_weather_code
hourly_data["sunshine_duration"] = hourly_sunshine_duration
hourly_data["is_day"] = hourly_is_day
hourly_data["uv_index_clear_sky"] = hourly_uv_index_clear_sky

hourly_dataframe = pd.DataFrame(data = hourly_data)
print("\nHourly data\n", hourly_dataframe)
