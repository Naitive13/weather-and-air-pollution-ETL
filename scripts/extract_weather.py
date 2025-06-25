from datetime import datetime

import requests
from get_coordinates import get_coordinates


def get_current_weather_data(city, api_key):
    coordinates = get_coordinates(city, api_key)
    open_weather_url = "https://api.openweathermap.org/data/2.5/weather"
    open_weather_params = {
        "lat": coordinates["lat"],
        "lon": coordinates["lon"],
        "appid": api_key,
        "units": "metric",
    }
    open_weather_response = requests.get(
        open_weather_url, params=open_weather_params, timeout=100
    ).json()

    actual_temperature = open_weather_response["main"]["temp"]
    weather = open_weather_response["weather"][0]["main"]
    description = open_weather_response["weather"][0]["description"]
    wind_speed = open_weather_response["wind"]["speed"]

    if "rain" in open_weather_response.keys():
        rain = open_weather_response["rain"]["1h"]
    else:
        rain = 0.00

    if "snow" in open_weather_response.keys():
        snow = open_weather_response["snow"]["1h"]
    else:
        snow = 0.00

    data = {
        "name": city,
        "weather": weather,
        "description": description,
        "temperature": actual_temperature,
        "wind speed": wind_speed,
        "rain": rain,
        "snow": snow,
        "date time": datetime.now().strftime("%Y-%m-%d"),
    }

    return data
