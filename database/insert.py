"""
Database insertion helpers.

This module exposes two helpers:
- `dbcon()`: constructs a psycopg2 connection using environment
  variables for host/user/password and a fixed `weather` database name.
- `insert_weather_data()`: performs an upsert for the location row and
  batch inserts (with `ON CONFLICT` upserts) for hourly and daily
  observations using `psycopg2.extras.execute_values` for efficiency.

The function handles deduplication within a single batch so that
PostgreSQL does not attempt to update the same target row more than once
in a single `INSERT ... ON CONFLICT` statement.
"""

import os
import psycopg2
from psycopg2.extras import execute_values


def dbcon():
    """Create and return a psycopg2 connection.

    The connection parameters are read from environment variables:
    `PG_HOST`, `PG_USER`, `PG_PASSWORD`. The database name is hard-coded
    to `weather` because this application targets that specific DB.
    """
    return psycopg2.connect( 
        host=os.getenv("PG_HOST"),
        database="weather",
        user=os.getenv("PG_USER"),        
        password=os.getenv("PG_PASSWORD")
    )


def insert_weather_data(location_row, weather_hourly_rows, weather_daily_rows):
    """Insert or update location, then batch insert/upsert weather rows.

    Args:
        location_row (tuple): (name, latitude, longitude, timezone, typed_name)
        weather_hourly_rows (list): list of tuples matching hourly columns
        weather_daily_rows (list): list of tuples matching daily columns

    Behavior:
    1. Upsert into `locations` returning `id`.
    2. Prepend the `location_id` to each hourly/daily tuple.
    3. Deduplicate rows within the batch by (location_id, time/date)
       taking the last-seen row for any duplicate key.
    4. Use `execute_values` to perform efficient bulk INSERT ... ON CONFLICT
       upserts for both `weather_hourly` and `weather_daily` tables.
    """

    with dbcon() as conn:
        with conn.cursor() as cur:

            # 1. Upsert location and get its surrogate id.
            cur.execute("""
                INSERT INTO locations (name, latitude, longitude, timezone, typed_name)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (latitude, longitude)
                DO UPDATE SET
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    timezone = EXCLUDED.timezone
                RETURNING id
            """, location_row)

            location_id = cur.fetchone()[0]

            # 2. Prepend the foreign key to each row so they match the
            # INSERT column ordering used below.
            weather_hourly_rows = [
                (location_id, *row)
                for row in weather_hourly_rows
            ]

            weather_daily_rows = [
                (location_id, *row)
                for row in weather_daily_rows
            ]

            # 3. Deduplicate rows that would share the same conflict key
            # within the same batch. PostgreSQL will raise an error if a
            # single INSERT ... ON CONFLICT would try to update the same
            # target row more than once; keep the last-seen row for each key.
            if weather_hourly_rows:
                seen = {}
                for r in weather_hourly_rows:
                    # key = (location_id, observation_time)
                    key = (r[0], r[1])
                    seen[key] = r
                weather_hourly_rows = list(seen.values())

            if weather_daily_rows:
                seen = {}
                for r in weather_daily_rows:
                    # key = (location_id, date)
                    key = (r[0], r[1])
                    seen[key] = r
                weather_daily_rows = list(seen.values())

            # 4. Insert hourly batch with ON CONFLICT upsert on (location_id, observation_time)
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

            # 5. Insert daily batch with ON CONFLICT upsert on (location_id, date)
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

        # Commit the transaction once both batches have been processed.
        conn.commit()
