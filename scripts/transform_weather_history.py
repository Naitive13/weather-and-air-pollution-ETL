import os
import pandas as pd
import logging

from pathlib import Path

from .merge_historical_data import merge_historical_weather_data


def transform_historical_weather_data(city):
    path = Path(__file__).parent / "./weather-history"
    try:
        file_names = [
            file for file in os.listdir(path) if city.replace(" ", "_") in file
        ]
        frames = []
        for file in file_names:
            df = pd.read_csv(f"{path}/{file}", index_col=False)
            df["snowfall_sum (cm)"] = df["snowfall_sum (cm)"] * 10
            new_columns = {
                "time": "date time",
                "temperature_2m_mean (°C)": "temperature",
                "rain_sum (mm)": "rain",
                "snowfall_sum (cm)": "snow",
                "wind_speed_10m_max (m/s)": "wind speed",
            }
            df.rename(columns=new_columns, inplace=True)
            df = df[["date time", "temperature", "wind speed", "rain", "snow"]]
            df["name"] = [""] * 365
            df.replace("", city, inplace=True)
            frames.append(df)
        data = pd.concat(frames)
        merge_historical_weather_data(data, city)
        return True
    except Exception as e:
        logging.error(f"Failed to transform data for {city}: {str(e)}")
