import openmeteo_requests
import json
import pandas as pd
import requests_cache
from retry_requests import retry
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# def transform_postgres(weather_df):
# 	locations_rows = []
	
# 	for _, row in weather_df.iterrows():
# 		bool_day = (row['is_day'] == 1)
# 		weather_current_rows.append((

# 			row['date'],
# 			row['temperature_2m'],
# 			row['apparent_temperature'],
# 			row['rain'],
# 			row['snowfall'],
# 			row['snow_depth'],
# 			row['surface_pressure'],
# 			row['cloud_cover'],
# 			row['wind_speed_10m'],
# 			row['wind_gusts_10m'],
# 			row['wind_direction_10m'],
# 			row['soil_temperature_0_to_7cm'],
# 			row['soil_moisture_0_to_7cm'],
# 			row['sunshine_duration']
# 		))
# 	weather_rows = []
# 	for _, row in weather_df.iterrows():
# 		weather_rows.append((
# 			row['date'],
# 			row['temperature_2m'],
# 			row['relative_humidity_2m'],
# 			row['dew_point_2m'],
# 			row['apparent_temperature'],
# 			row['rain'],
# 			row['snowfall'],
# 			row['snow_depth'],
# 			row['surface_pressure'],
# 			row['cloud_cover'],
# 			row['wind_speed_10m'],
# 			row['wind_gusts_10m'],
# 			row['wind_direction_10m'],
# 			row['soil_temperature_0_to_7cm'],
# 			row['soil_moisture_0_to_7cm'],
# 			row['sunshine_duration']
# 		))
# 	return weather_rows

def get_weather(lat = None, lon = None, start_date = None, end_date = None):
	now = datetime.now().strftime("%Y-%m-%d")
	# Make sure all required weather variables are listed here
	# The order of variables in hourly or daily is important to assign them correctly below	
	if lat is None:
		lat = 37.78
	if lon is None:
		lon = -81.19
	if start_date is None:
		start_date = now
	if end_date is None:
		end_date = now

	# Setup the Open-Meteo API client with cache and retry on error
	cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
	retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
	openmeteo = openmeteo_requests.Client(session = retry_session)

	url = "https://archive-api.open-meteo.com/v1/archive"
	params = {
		"latitude": lat,#37.78,
		"longitude": lon,#-81.19,
		"start_date": start_date,
		"end_date": end_date,
		"hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "rain", "snowfall", "snow_depth", "surface_pressure", "cloud_cover", "wind_speed_10m", "wind_gusts_10m", "wind_direction_10m", "soil_temperature_0_to_7cm", "soil_moisture_0_to_7cm", "sunshine_duration"],
		"timezone": "America/New_York",
		"temperature_unit": "fahrenheit",
		"wind_speed_unit": "mph",
		"precipitation_unit": "inch",
	}
	responses = openmeteo.weather_api(url, params=params)

	# Process first location. Add a for-loop for multiple locations or weather models
	response = responses[0]
	# print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
	# print(f"Elevation: {response.Elevation()} m asl")
	# print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

	# Process hourly data. The order of variables needs to be the same as requested.
	hourly = response.Hourly()
	hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
	hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
	hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
	hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
	hourly_rain = hourly.Variables(4).ValuesAsNumpy()
	hourly_snowfall = hourly.Variables(5).ValuesAsNumpy()
	hourly_snow_depth = hourly.Variables(6).ValuesAsNumpy()
	hourly_surface_pressure = hourly.Variables(7).ValuesAsNumpy()
	hourly_cloud_cover = hourly.Variables(8).ValuesAsNumpy()
	hourly_wind_speed_10m = hourly.Variables(9).ValuesAsNumpy()
	hourly_wind_gusts_10m = hourly.Variables(10).ValuesAsNumpy()
	hourly_wind_direction_10m = hourly.Variables(11).ValuesAsNumpy()
	hourly_soil_temperature_0_to_7cm = hourly.Variables(12).ValuesAsNumpy()
	hourly_soil_moisture_0_to_7cm = hourly.Variables(13).ValuesAsNumpy()
	hourly_sunshine_duration = hourly.Variables(14).ValuesAsNumpy()

	hourly_data = {"date": pd.date_range(
		start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
		end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
		freq = pd.Timedelta(seconds = hourly.Interval()),
		inclusive = "left"
	)}

	hourly_data["temperature_2m"] = hourly_temperature_2m
	hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
	hourly_data["dew_point_2m"] = hourly_dew_point_2m
	hourly_data["apparent_temperature"] = hourly_apparent_temperature
	hourly_data["rain"] = hourly_rain
	hourly_data["snowfall"] = hourly_snowfall
	hourly_data["snow_depth"] = hourly_snow_depth
	hourly_data["surface_pressure"] = hourly_surface_pressure
	hourly_data["cloud_cover"] = hourly_cloud_cover
	hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
	hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m
	hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
	hourly_data["soil_temperature_0_to_7cm"] = hourly_soil_temperature_0_to_7cm
	hourly_data["soil_moisture_0_to_7cm"] = hourly_soil_moisture_0_to_7cm
	hourly_data["sunshine_duration"] = hourly_sunshine_duration

	hourly_dataframe = pd.DataFrame(data = hourly_data)

	return hourly_dataframe, start_date, end_date



# df = get_weather()
# print(df.describe())
# print(df.info())
# print(df.head())
# print(df.tail())

# print(f"soil temp: \n{df['soil_moisture_0_to_7cm']}")

