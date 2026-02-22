"""
Fetch weather data from the Open-Meteo archive API and return it as
Pandas DataFrames for hourly and daily series.

This module uses `openmeteo_requests` (a client wrapper around the API
response format) and `requests_cache` plus `retry_requests` to make the
calls resilient and cache responses locally.
"""

import openmeteo_requests
import json
import pandas as pd
import requests_cache
from retry_requests import retry
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime


def get_weather(lat = None, lon = None, start_date = None, end_date = None):
	"""Fetch hourly and daily weather for a single location.

	Args:
		lat (float): Latitude of location. Defaults to a hard-coded value.
		lon (float): Longitude of location. Defaults to a hard-coded value.
		start_date (str): YYYY-MM-DD start date. Defaults to today.
		end_date (str): YYYY-MM-DD end date. Defaults to today.

	Returns:
		tuple: (hourly_dataframe, daily_dataframe, start_date, end_date)

	Notes:
		- The order of variables in `hourly` and `daily` must match the
		  extraction order below because the Open-Meteo client exposes
		  variables as positional indices.
		- The function currently processes only the first response
		  (`responses[0]`) — it can be extended to loop multiple
		  locations/models if needed.
	"""

	now = datetime.now().strftime("%Y-%m-%d")

	# Use sensible defaults when callers omit parameters.
	if lat is None:
		lat = 37.78
	if lon is None:
		lon = -81.19
	if start_date is None:
		start_date = now
	if end_date is None:
		end_date = now

	# Configure a cached requests session and wrap it with retry logic to
	# avoid transient network failures. The cache directory is '.cache'.
	cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
	retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
	openmeteo = openmeteo_requests.Client(session = retry_session)

	# Build API parameters: list all desired hourly and daily variables.
	url = "https://archive-api.open-meteo.com/v1/archive"
	params = {
		"latitude": lat,
		"longitude": lon,
		"start_date": start_date,
		"end_date": end_date,
		"hourly": [
			"temperature_2m",
			"relative_humidity_2m",
			"dew_point_2m",
			"apparent_temperature",
			"rain",
			"snowfall",
			"snow_depth",
			"surface_pressure",
			"cloud_cover",
			"wind_speed_10m",
			"wind_gusts_10m",
			"wind_direction_10m",
			"soil_temperature_0_to_7cm",
			"soil_moisture_0_to_7cm",
		],
		"daily": [
			"precipitation_sum",
			"temperature_2m_max",
			"temperature_2m_min",
			"sunrise",
			"sunset",
			"wind_gusts_10m_max",
			"wind_speed_10m_max",
			"wind_direction_10m_dominant",
		],
		"timezone": "America/New_York",
		"temperature_unit": "fahrenheit",
		"wind_speed_unit": "mph",
		"precipitation_unit": "inch",
	}

	# Make the API call via the client wrapper.
	responses = openmeteo.weather_api(url, params=params)

	# Process only the first response for this single-location helper.
	response = responses[0]

	# --- DAILY ---
	# The client exposes daily variables via positional indices; extract
	# each variable into numpy arrays and then assemble a DataFrame.
	daily = response.Daily()
	daily_precipitation_sum = daily.Variables(0).ValuesAsNumpy()
	daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
	daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
	daily_sunrise = daily.Variables(3).ValuesInt64AsNumpy()
	daily_sunset = daily.Variables(4).ValuesInt64AsNumpy()
	daily_wind_gusts_10m_max = daily.Variables(5).ValuesAsNumpy()
	daily_wind_speed_10m_max = daily.Variables(6).ValuesAsNumpy()
	daily_wind_direction_10m_dominant = daily.Variables(7).ValuesAsNumpy()
	daily_times = daily.Time()

	# Create a DataFrame for daily series. Note: sunrise/sunset are parsed
	# as epoch seconds and converted to timezone-aware datetimes below.
	daily_data = pd.DataFrame({
		"date": pd.to_datetime(daily_times, unit="s", utc=True),
		"sunrise": pd.to_datetime(daily_sunrise, unit="s", utc=True),
		"sunset": pd.to_datetime(daily_sunset, unit="s", utc=True),
	})
	daily_data["precipitation_sum"] = daily_precipitation_sum
	daily_data["temperature_2m_max"] = daily_temperature_2m_max
	daily_data["temperature_2m_min"] = daily_temperature_2m_min
	daily_data["sunrise"] = daily_sunrise
	daily_data["sunset"] = daily_sunset
	daily_data["wind_gusts_10m_max"] = daily_wind_gusts_10m_max
	daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
	daily_data["wind_direction_10m_dominant"] = daily_wind_direction_10m_dominant

	daily_dataframe = pd.DataFrame(data = daily_data)

	# --- HOURLY ---
	# Extract hourly variables in the same ordered manner as requested
	# (positional indices must match the `hourly` list above).
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

	# Build the hourly datetime index using the client's Time/TimeEnd/Interval
	# information and create a dictionary of series that will become a DataFrame.
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

	hourly_dataframe = pd.DataFrame(data = hourly_data)

	# Return both DataFrames and echo the (possibly defaulted) date range.
	return hourly_dataframe, daily_dataframe, start_date, end_date


