from fastapi import FastAPI, HTTPException
import requests
from ingest.geocode import geocode_city

app = FastAPI()

# Code	Description
# 0	Clear sky
# 1, 2, 3	Mainly clear, partly cloudy, and overcast
# 45, 48	Fog and depositing rime fog
# 51, 53, 55	Drizzle: Light, moderate, and dense intensity
# 56, 57	Freezing Drizzle: Light and dense intensity
# 61, 63, 65	Rain: Slight, moderate and heavy intensity
# 66, 67	Freezing Rain: Light and heavy intensity
# 71, 73, 75	Snow fall: Slight, moderate, and heavy intensity
# 77	Snow grains
# 80, 81, 82	Rain showers: Slight, moderate, and violent
# 85, 86	Snow showers slight and heavy
# 95 *	Thunderstorm: Slight or moderate
# 96, 99 *	Thunderstorm with slight and heavy hail

weather_code_map = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle:",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    56: "Light Freezing Drizzle",
    57: "Dense Freezing Drizzle:",
    61: "Slight Rain",
    63: "Moderate Rain",
    65: "Heavy Rain",
    66: "Light Freezing Rain",
    67: "Heavy Freezing Rain",
    71: "Slight Snow fall",
    73: "Moderate Snow fall",
    75: "Heavy Snow fall",
    77: "Snow grains",
    80: "Slight Rain showers",
    81: "Moderate Rain showers",
    82: "Violent Rain showers",
    85: "Slight Snow showers",  
    86: "Heavy Snow showers",
    95: "Slight Thunderstorm",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail"
}

def deg_to_compass(deg):
    try:
        d = float(deg)
    except (TypeError, ValueError):
        return None
    directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    idx = int((d / 45.0) + 0.5) % 8
    return directions[idx]

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
                "wind_speed_unit": "mph",
                "temperature_unit": "fahrenheit",
                "precipitation_unit": "inch",
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

        # wc = at("weather_code")
        # weather_desc = weather_code_map.get(wc, "Unknown") if wc is not None else None
            
        forecast_data.append({
            "date": at("time"),
            "weather_code": weather_code_map.get(at("weather_code"), "Unknown") if at("weather_code") is not None else None,
            "max_temp": at("temperature_2m_max"),
            "min_temp": at("temperature_2m_min"),
            "sunrise": at("sunrise"),
            "sunset": at("sunset"),
            "uv_index_max": at("uv_index_max"),
            "precipitation_sum": round(at("precipitation_sum"),2),
            "precipitation_probability_max": round(at("precipitation_probability_max"),2),
            "precipitation_hours": at("precipitation_hours"),
            "snowfall_sum": at("snowfall_sum"),
            "showers_sum": at("showers_sum"),
            "rain_sum": at("rain_sum"),
            "wind_speed_max": at("wind_speed_10m_max"),
            "wind_gusts_max": at("wind_gusts_10m_max"),
            "wind_direction": deg_to_compass(at("wind_direction_10m_dominant")),
            "shortwave_radiation_sum": at("shortwave_radiation_sum"),
            "evapotranspiration": at("et0_fao_evapotranspiration"),
            "daylight_duration": at("daylight_duration"),
            "sunshine_duration": at("sunshine_duration"),
            "uv_index_clear_sky_max": at("uv_index_clear_sky_max"),
            "feels_like_max": round(at("apparent_temperature_max"),2),
            "feels_like_min": round(at("apparent_temperature_min"),2)                                                              
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