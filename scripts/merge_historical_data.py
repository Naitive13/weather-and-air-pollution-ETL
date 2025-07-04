import logging
from pathlib import Path

import gspread
from oauth2client.service_account import ServiceAccountCredentials


def merge_historical_air_pollution_data(data, city):
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive",
        ]

        path = Path(__file__).parent / "../client_secret.json"
        credentials = ServiceAccountCredentials.from_json_keyfile_name(path, scope)
        client = gspread.authorize(credentials)
        spreadsheet = client.open("Air-pollution-ETL").worksheet(
            f"History-{city.replace(' ', '_')}"
        )
        spreadsheet.append_row(data)
        return True
    except Exception as e:
        logging.error(f"Error while merging air pollution data: {str(e)}")
        return False


def merge_historical_weather_data(data, city):
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive",
        ]

        path = Path(__file__).parent / "../client_secret.json"
        credentials = ServiceAccountCredentials.from_json_keyfile_name(path, scope)
        client = gspread.authorize(credentials)
        spreadsheet = client.open("Weather-ETL").worksheet(
            f"History-{city.replace(' ', '_')}"
        )
        spreadsheet.append_rows(data)
        return True
    except Exception as e:
        logging.error(f"Error while merging weather data: {str(e)}")
        return False
