from fastapi import FastAPI, HTTPException
import requests
from ingest.geocode import geocode_city

app = FastAPI()

@app.get("/current")
def current_weather(city: str):

    try:
        location = resolve_city(city)
    except ValueError:
        raise HTTPException(status_code=404, detail="City not found")
    except Exception:
        raise HTTPException(status_code=500, detail="Geocoding service error")

    forecast_url = "https://api.open-meteo.com/v1/forecast"

    weather_response = requests.get(
        forecast_url,
        params={
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "current_weather": True,
            "temperature_unit": "fahrenheit",
            "wind_speed_unit": "mph"
        }
    ).json()

    current = weather_response["current_weather"]

    return {
        "city": location["name"],
        "temperature": current["temperature"],
        "windspeed": current["windspeed"],
        "time": current["time"]
    }

@app.get("/forecast")
def forecast(city: str):

    try:
        location = resolve_city(city)
    except ValueError:
        raise HTTPException(status_code=404, detail="City not found")
    except Exception:
        raise HTTPException(status_code=500, detail="Geocoding service error")

    forecast_url = "https://api.open-meteo.com/v1/forecast"
    weather_response = requests.get(
        forecast_url,
        params={
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "sunrise", "sunset", "uv_index_max", "precipitation_sum", "precipitation_probability_max", "precipitation_hours", "snowfall_sum", "showers_sum", "rain_sum", "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum", "et0_fao_evapotranspiration", "daylight_duration", "sunshine_duration", "uv_index_clear_sky_max", "apparent_temperature_max", "apparent_temperature_min"],
            "temperature_unit": "fahrenheit",
            "forecast_days": 5,
            "timezone": "auto"
        }
    ).json()

    daily = weather_response["daily"]

    forecast_data = []

    for i in range(len(daily["time"])):
        forecast_data.append({
            "date": daily["time"][i],
            "weather_code": daily["weather_code"][i],
            "max_temp": daily["temperature_2m_max"][i],
            "min_temp": daily["temperature_2m_min"][i],
            "sunrise": daily["sunrise"][i],
            "sunset": daily["sunset"][i],
            "uv_index_max": daily["uv_index_max"][i],
            "precipitation_sum": daily["precipitation_sum"][i],
            "precipitation_probability_max": daily["precipitation_probability_max"][i],
            "precipitation_hours": daily["precipitation_hours"][i],
            "snowfall_sum": daily["snowfall_sum"][i],
            "showers_sum": daily["showers_sum"][i],
            "rain_sum": daily["rain_sum"][i],
            "wind_speed_max": daily["wind_speed_10m_max"][i],
            "wind_gusts_max": daily["wind_gusts_10m_max"][i],
            "wind_direction": daily["wind_direction_10m_dominant"][i],
            "shortwave_radiation_sum": daily["shortwave_radiation_sum"][i],
            "evapotranspiration": daily["et0_fao_evapotranspiration"][i],
            "daylight_duration": daily["daylight_duration"][i],
            "sunshine_duration": daily["sunshine_duration"][i],
            "uv_index_clear_sky_max": daily["uv_index_clear_sky_max"][i],
            "feels_like_max": daily["apparent_temperature_max"][i],
            "feels_like_min": daily["apparent_temperature_min"][i]                                                              
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