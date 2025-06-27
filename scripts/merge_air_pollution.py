import gspread
import logging
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials


def merge_weather_data(cities):
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive",
        ]

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            "../client_secret.json", scope
        )
        client = gspread.authorize(credentials)
        spreadsheet = client.open("Air-pollution-ETL").worksheet("Air-pollution-ETL")

        for city in cities:
            try:
                df = pd.read_csv(
                    f"../data/air_pollution/air_pollution_{city}", index_col=False
                )
                for index, row in df.iterrows():
                    spreadsheet.append_row(row.to_list())
            except FileNotFoundError:
                logging.error(f"file 'air_pollution_{city}' not found, skipping...")
                continue
            except Exception as e:
                logging.error(f"Error processing {city}: {str(e)}")
                continue

        return True
    except Exception as e:
        print(f"Error while merging air pollution data: {str(e)}")
        return False
