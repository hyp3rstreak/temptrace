from datetime import datetime, timezone

def transform_weather_data(df_hourly, df_daily, geocoded_city):

    weather_hourly_rows = []
    weather_daily_rows = []

    # --- DAILY ---
    for _, row in df_daily.iterrows():
        sunrise_dt = datetime.fromtimestamp(row['sunrise'], tz=timezone.utc)
        sunset_dt = datetime.fromtimestamp(row['sunset'], tz=timezone.utc)

        weather_daily_rows.append((
            row['date'],                    # date
            sunrise_dt,                     # sunrise
            sunset_dt,                      # sunset
            row['precipitation_sum'],       # precipitation_sum
            row['temperature_2m_max'],      # temp_max
            row['temperature_2m_min'],      # temp_min
            row['wind_gusts_10m_max'],      # gusts
            row['wind_speed_10m_max'],      # wind speed
            row['wind_direction_10m_dominant']
        ))

    # --- HOURLY ---
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

    location_row = (
        geocoded_city['name'],
        geocoded_city['latitude'],
        geocoded_city['longitude'],
        geocoded_city['timezone']
    )

    return location_row, weather_hourly_rows, weather_daily_rows