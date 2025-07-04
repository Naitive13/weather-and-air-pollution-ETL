from datetime import datetime

import requests
import logging

from .merge_historical_data import (
    merge_historical_air_pollution_data,
)
from .get_coordinates import get_coordinates


def get_historical_air_pollution(city, api_key):
    try:
        coordinates = get_coordinates(city, api_key)
        url = "http://api.openweathermap.org/data/2.5/air_pollution/history"
        params = {
            "lat": coordinates["lat"],
            "lon": coordinates["lon"],
            "appid": api_key,
            "start": 1672531200,
            "end": 1751014800,
        }
        response = requests.get(url, params=params, timeout=100).json()
        # print(response)
        previous_date = ""
        data = []
        for item in response["list"]:
            date = datetime.utcfromtimestamp(item["dt"]).strftime("%Y-%m-%d")
            if date != previous_date:
                previous_date = date
                air_quality_index = item["main"]["aqi"]
                carbon_monoxide = item["components"]["co"]
                nitrogen_dioxide = item["components"]["no2"]
                ozone = item["components"]["o3"]
                particulate_matter_2_5 = item["components"]["pm2_5"]
                particulate_matter_10 = item["components"]["pm10"]
                row = [
                    city,
                    date,
                    air_quality_index,
                    carbon_monoxide,
                    nitrogen_dioxide,
                    ozone,
                    particulate_matter_2_5,
                    particulate_matter_10,
                ]
                data.append(row)

        merge_historical_air_pollution_data(data, city)
        logging.info(f"Historical data for {city} extracted!")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data for city '{city}': {str(e)}")
    except Exception as e:
        logging.error(f"Extraction failed for city '{city}': {str(e)}")
    return False
