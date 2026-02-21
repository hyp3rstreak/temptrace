import requests

def geocode_city(city_name = "New York City"):
    if city_name is None:
        city_name = "New York City"
    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {
        "name": city_name,
        "count": 1
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
    if "results" not in data:
        raise ValueError(f"City '{city_name}' not found in geocoding API.")
    
    result = data["results"][0]
    
    return {
        "name": result["name"],
        "latitude": result["latitude"],
        "longitude": result["longitude"],
        "timezone": result["timezone"]
    }