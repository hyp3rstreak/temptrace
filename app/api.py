from fastapi import FastAPI, HTTPException
import requests

app = FastAPI()

@app.get("/current")
def current_weather(city: str):

    geo_url = "https://geocoding-api.open-meteo.com/v1/search"
    geo_response = requests.get(
        geo_url,
        params={"name": city, "count": 1}
    ).json()

    if "results" not in geo_response:
        raise HTTPException(status_code=404, detail="City not found")

    location = geo_response["results"][0]

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