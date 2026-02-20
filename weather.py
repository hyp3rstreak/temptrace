import requests
import json
import os
from datetime import datetime

API_KEY = "62224b7285f32a1bb51ba1889fd728bc"
lat = 37.78
lon = -81.19
today = datetime.today().date().isoformat()

try:
    r = requests.get(f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}")
    rjson = r.json
except requests.exceptions.RequestException as e:
    print(f"Error fetching weather data: {e}")
    
print(f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}")

with open("weather_.json", "w") as f:
    json.dumps(rjson, f, indent=4)