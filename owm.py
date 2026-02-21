from pyowm import OWM
from pyowm.utils import config
from pyowm.utils import timestamps

# ---------- FREE API KEY examples ---------------------

owm = OWM('62224b7285f32a1bb51ba1889fd728bc')
mgr = owm.weather_manager()


# Search for current weather in London (Great Britain) and get details
observation = mgr.weather_at_place('West Virginia,US')
w = observation.weather

print("Sunrise: ", w.sunrise_time())
print("Sunrise: ", w.sunrise_time(timeformat='iso'))
print("Sunrise: ", w.sunrise_time(timeformat='date'))
print("Sunset: ", w.sunset_time())
print("Current status: ", w.detailed_status)         # 'clouds'
print("Wind gust: ", w.wind()['gust'])                  # {'speed': 4.6, 'deg': 330}
print("Humidity: ", w.humidity)                # 87
print("Temp: ", w.temperature('fahrenheit')['temp'])
print("Feels like: ", w.temperature('fahrenheit')['feels_like'])  # {'temp_max': 10.5, 'temp': 9.7, 'temp_min': 9.0}
print("Rain: ", w.rain)                    # {}
print("Heat: ", w.heat_index)              # None
print("Clouds: ", w.clouds, "%")                  # 75