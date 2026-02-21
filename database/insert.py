import os
import psycopg2
from psycopg2.extras import execute_values

def dbcon():
    return psycopg2.connect( 
        host=os.getenv("PG_HOST"),
        database="weather",
        user=os.getenv("PG_USER"),        
        password=os.getenv("PG_PASSWORD")
    )

def insert_weather_data(location_row, weather_hourly_rows, weather_daily_rows):
    print(type(location_row))
    print("location_rows:", location_row)
    print("length:", len(location_row))
    with dbcon() as conn:
        with conn.cursor() as cur:

            # 1. Upsert location
            cur.execute("""
                INSERT INTO locations (name, latitude, longitude, timezone)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (latitude, longitude)
                DO UPDATE SET
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    timezone = EXCLUDED.timezone
                RETURNING id
            """, location_row)

            location_id = cur.fetchone()[0]

            # 2. Inject foreign key
            weather_hourly_rows = [
                (location_id, *row)
                for row in weather_hourly_rows
            ]

            weather_daily_rows = [
                (location_id, *row)
                for row in weather_daily_rows
            ]
            print("weather_hourly_rows sample:", weather_hourly_rows[0])
            # 3. Insert hourly
            execute_values(
                cur,
                """
                INSERT INTO weather_hourly (
                    location_id,
                    observation_time,
                    temperature_2m,
                    relative_humidity_2m,
                    dew_point_2m,
                    apparent_temperature,
                    rain,
                    snowfall,
                    snow_depth,
                    surface_pressure,
                    cloud_cover,
                    wind_speed_10m,
                    wind_gusts_10m,
                    wind_direction_10m,
                    soil_temperature_0_to_7cm,
                    soil_moisture_0_to_7cm
                )
                VALUES %s
                ON CONFLICT (location_id, observation_time)
                DO UPDATE SET
                    temperature_2m = EXCLUDED.temperature_2m
                """,
                weather_hourly_rows
            )

            # 4. Insert daily
            execute_values(
                cur,
                """
                INSERT INTO weather_daily (
                    location_id,
                    date,
                    sunrise,
                    sunset,
                    precipitation_sum,
                    temperature_2m_max,
                    temperature_2m_min,
                    wind_gusts_10m_max,
                    wind_speed_10m_max,
                    wind_direction_10m_dominant
                )
                VALUES %s
                ON CONFLICT (location_id, date)
                DO UPDATE SET
                    precipitation_sum = EXCLUDED.precipitation_sum
                """,
                weather_daily_rows
            )

        conn.commit()