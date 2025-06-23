from datetime import datetime

import requests
from get_coordinates import get_coordinates


def get_current_air_pollution_data(city, api_key):
    coordinates = get_coordinates(city, api_key)
    air_pollution_url = "http://api.openweathermap.org/data/2.5/air_pollution"
    air_pollution_params = {
        "lat": coordinates["lat"],
        "lon": coordinates["lon"],
        "appid": api_key,
    }
    air_pollution_response = requests.get(
        air_pollution_url, params=air_pollution_params, timeout=10
    ).json()

    air_quality_index = air_pollution_response["list"][0]["main"]["aqi"]
    carbon_monoxide = air_pollution_response["list"][0]["components"]["co"]
    nitrogen_dioxide = air_pollution_response["list"][0]["components"]["no2"]
    ozone = air_pollution_response["list"][0]["components"]["o3"]
    particulate_matter_2_5 = air_pollution_response["list"][0]["components"]["pm2_5"]
    particulate_matter_10 = air_pollution_response["list"][0]["components"]["pm10"]

    data = {
        "air quality index": air_quality_index,
        "carbon monoxide": carbon_monoxide,
        "nitrogen dioxide": nitrogen_dioxide,
        "ozone": ozone,
        "particulate matter 2.5": particulate_matter_2_5,
        "particulate matter 10": particulate_matter_10,
        "date time": datetime.now().strftime("%Y-%m-%d"),
    }

    return data
