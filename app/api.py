from fastapi import FastAPI
import requests

app = FastAPI()


@app.get("/current")
def current_weather(city: str):

    # 1. Geocode city
    geo_url = "https://geocoding-api.open-meteo.com/v1/search"
    geo_response = requests.get(
        geo_url,
        params={"name": city, "count": 1}
    ).json()

    if "results" not in geo_response:
        return {"error": "City not found"}

    location = geo_response["results"][0]

    # 2. Fetch current weather
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
    print(weather_response)
    current = weather_response["current_weather"]

    return {
        "city": location["name"],
        "temperature": current["temperature"],
        "windspeed": current["windspeed"],
        "time": current["time"]
    }