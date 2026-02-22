from fastapi import FastAPI, HTTPException
import requests
from ingest.geocode import geocode_city

app = FastAPI()

@app.get("/current")
def current_weather(city: str):
    # Let resolve_city raise HTTPException for not-found or internal errors
    location = resolve_city(city)

    forecast_url = "https://api.open-meteo.com/v1/forecast"

    try:
        resp = requests.get(
            forecast_url,
            params={
                "latitude": location["latitude"],
                "longitude": location["longitude"],
                "current_weather": True,
                "temperature_unit": "fahrenheit",
                "wind_speed_unit": "mph"
            },
            timeout=10
        )
        resp.raise_for_status()
        weather_response = resp.json()
    except requests.RequestException:
        raise HTTPException(status_code=502, detail="Upstream weather service error")
    except ValueError:
        raise HTTPException(status_code=502, detail="Invalid response from weather service")

    if "current_weather" not in weather_response:
        raise HTTPException(status_code=502, detail="Upstream response missing current_weather")

    current = weather_response["current_weather"]

    return {
        "city": location["name"],
        "temperature": current.get("temperature"),
        "windspeed": current.get("windspeed"),
        "time": current.get("time")
    }

@app.get("/forecast")
def forecast(city: str):
    # Let resolve_city raise HTTPException for not-found or internal errors
    location = resolve_city(city)

    forecast_url = "https://api.open-meteo.com/v1/forecast"
    try:
        resp = requests.get(
            forecast_url,
            params={
                "latitude": location["latitude"],
                "longitude": location["longitude"],
                "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "sunrise", "sunset", "uv_index_max", "precipitation_sum", "precipitation_probability_max", "precipitation_hours", "snowfall_sum", "showers_sum", "rain_sum", "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum", "et0_fao_evapotranspiration", "daylight_duration", "sunshine_duration", "uv_index_clear_sky_max", "apparent_temperature_max", "apparent_temperature_min"],
                "temperature_unit": "fahrenheit",
                "forecast_days": 5,
                "timezone": "auto"
            },
            timeout=10
        )
        resp.raise_for_status()
        weather_response = resp.json()
    except requests.RequestException:
        raise HTTPException(status_code=502, detail="Upstream weather service error")
    except ValueError:
        raise HTTPException(status_code=502, detail="Invalid response from weather service")

    if "daily" not in weather_response:
        raise HTTPException(status_code=502, detail="Upstream response missing daily data")

    daily = weather_response["daily"]

    forecast_data = []

    times = daily.get("time", [])

    for i in range(len(times)):
        def at(key):
            vals = daily.get(key, [])
            return vals[i] if i < len(vals) else None

        forecast_data.append({
            "date": at("time"),
            "weather_code": at("weather_code"),
            "max_temp": at("temperature_2m_max"),
            "min_temp": at("temperature_2m_min"),
            "sunrise": at("sunrise"),
            "sunset": at("sunset"),
            "uv_index_max": at("uv_index_max"),
            "precipitation_sum": at("precipitation_sum"),
            "precipitation_probability_max": at("precipitation_probability_max"),
            "precipitation_hours": at("precipitation_hours"),
            "snowfall_sum": at("snowfall_sum"),
            "showers_sum": at("showers_sum"),
            "rain_sum": at("rain_sum"),
            "wind_speed_max": at("wind_speed_10m_max"),
            "wind_gusts_max": at("wind_gusts_10m_max"),
            "wind_direction": at("wind_direction_10m_dominant"),
            "shortwave_radiation_sum": at("shortwave_radiation_sum"),
            "evapotranspiration": at("et0_fao_evapotranspiration"),
            "daylight_duration": at("daylight_duration"),
            "sunshine_duration": at("sunshine_duration"),
            "uv_index_clear_sky_max": at("uv_index_clear_sky_max"),
            "feels_like_max": at("apparent_temperature_max"),
            "feels_like_min": at("apparent_temperature_min")                                                              
        })

    return {
        "city": location["name"],
        "forecast": forecast_data
    }

def resolve_city(city: str):
    try:
        return geocode_city(city)
    except ValueError:
        raise HTTPException(status_code=404, detail="City not found")
    except Exception:
        raise HTTPException(status_code=500, detail="Geocoding service error")