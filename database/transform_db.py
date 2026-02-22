"""
Transform Pandas DataFrames into tuples suitable for database insertion.

This module converts the hourly and daily DataFrames produced by
`ingest.fetch.get_weather` into lists of tuples that match the
database schema expected by `database.insert.insert_weather_data`.

The output is a tuple of three items:
 - `location_row`: a 5-element tuple with location metadata
 - `weather_hourly_rows`: a list of tuples for hourly observations
 - `weather_daily_rows`: a list of tuples for daily summaries

Important: this module does not perform any DB I/O; it only prepares
Python-native structures for insertion. It also converts epoch
second sunrise/sunset values into timezone-aware `datetime` objects.
"""

from datetime import datetime, timezone


def transform_weather_data(df_hourly, df_daily, geocoded_city):
    """Convert DataFrame rows into DB-ready tuples.

    Args:
        df_hourly (pd.DataFrame): Hourly observations with a `date` index
        df_daily (pd.DataFrame): Daily aggregated data with `date` column
        geocoded_city (dict): Location metadata from `geocode_city`

    Returns:
        tuple: (`location_row`, `weather_hourly_rows`, `weather_daily_rows`)

    The function preserves the order of fields to match the INSERT
    statements in `database.insert` so that `execute_values` can
    efficiently insert the batches without additional mapping logic.
    """

    weather_hourly_rows = []
    weather_daily_rows = []

    # --- DAILY ---
    # Iterate daily DataFrame rows and convert epoch seconds (sunrise/sunset)
    # into aware datetimes. Each appended tuple matches the weather_daily
    # table column order used by the inserter.
    for _, row in df_daily.iterrows():
        sunrise_dt = datetime.fromtimestamp(row['sunrise'], tz=timezone.utc)
        sunset_dt = datetime.fromtimestamp(row['sunset'], tz=timezone.utc)

        weather_daily_rows.append((
            row['date'],                    # date
            sunrise_dt,                     # sunrise (aware datetime)
            sunset_dt,                      # sunset (aware datetime)
            row['precipitation_sum'],       # precipitation_sum
            row['temperature_2m_max'],      # temperature max
            row['temperature_2m_min'],      # temperature min
            row['wind_gusts_10m_max'],      # max gusts
            row['wind_speed_10m_max'],      # max wind speed
            row['wind_direction_10m_dominant']
        ))

    # --- HOURLY ---
    # Walk the hourly DataFrame and produce tuples in the exact order
    # expected by the `weather_hourly` INSERT statement.
    for _, row in df_hourly.iterrows():
        weather_hourly_rows.append((
            row['date'],
            row['temperature_2m'],
            row['relative_humidity_2m'],
            row['dew_point_2m'],
            row['apparent_temperature'],
            row['rain'],
            row['snowfall'],
            row['snow_depth'],
            row['surface_pressure'],
            row['cloud_cover'],
            row['wind_speed_10m'],
            row['wind_gusts_10m'],
            row['wind_direction_10m'],
            row['soil_temperature_0_to_7cm'],
            row['soil_moisture_0_to_7cm']
        ))

    # Build a compact location tuple used to upsert the locations table.
    # The order here matches the `INSERT INTO locations (...)` parameters.
    location_row = (
        geocoded_city['name'],
        geocoded_city['latitude'],
        geocoded_city['longitude'],
        geocoded_city['timezone'],
        geocoded_city['typed_name'],
        geocoded_city['population'],
        geocoded_city['elevation'],
        geocoded_city['country'],
        geocoded_city['state']
    )

    return location_row, weather_hourly_rows, weather_daily_rows