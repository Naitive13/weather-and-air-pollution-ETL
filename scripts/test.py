from extract_weather import get_current_weather_data
from datetime import datetime
import pandas as pd

api_key = "d0dd659e37a715c6ba8b63a4bf3e6228"
weather_data = {
    "name": [],
    "weather": [],
    "description": [],
    "temperature": [],
    "rain": [],
    "snow": [],
    "date time": [],
}
cities = [
    "Antananarivo",
    "London",
    "Paris",
    "Tokyo",
    "Madrid",
    "Sydney",
    "New York",
    "Melbourne",
    "Seoul",
    "Manila",
    "Toronto",
]

for city in cities:
    city_data = get_current_weather_data(city, api_key)
    weather_data["name"].append(city_data["name"])
    weather_data["temperature"].append(city_data["temperature"])
    weather_data["weather"].append(city_data["weather"])
    weather_data["description"].append(city_data["description"])
    weather_data["rain"].append(city_data["rain"])
    weather_data["snow"].append(city_data["snow"])
    weather_data["date time"].append(city_data["date time"])

weather_df = pd.DataFrame(weather_data)
weather_df.to_csv(f"../data/weather_data_{datetime.now().strftime('%Y-%m-%d')}")
print(weather_df)
