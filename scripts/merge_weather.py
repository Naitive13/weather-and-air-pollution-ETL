import gspread
import logging
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from pathlib import Path


def merge_weather_data(cities):
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
        spreadsheet = client.open("Weather-ETL").worksheet("Weather-ETL")

        for city in cities:
            try:
                df = pd.read_csv(f"../data/weather_{city}", index_col=False)
                for index, row in df.iterrows():
                    spreadsheet.append_row(row.to_list())
            except FileNotFoundError:
                logging.error(f"file 'weather_{city}' not found, skipping...")
                continue
            except Exception as e:
                logging.error(f"Error processing {city}: {str(e)}")
                continue

        return True
    except Exception as e:
        logging.error(f"Error while merging weather data: {str(e)}")
        return False
