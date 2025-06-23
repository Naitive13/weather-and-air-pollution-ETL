import requests


def get_coordinates(city, api_key):
    geocoding_url = "http://api.openweathermap.org/geo/1.0/direct"
    geocoding_parms = {"q": city, "limit": 1, "appid": api_key}
    geocoding_response = requests.get(
        geocoding_url, params=geocoding_parms, timeout=10
    ).json()
    latitude = geocoding_response[0]["lat"]
    longitude = geocoding_response[0]["lon"]
    return {"lon": longitude, "lat": latitude}
