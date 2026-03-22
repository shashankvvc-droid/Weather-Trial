import sys # <-- IMPORTED FOR REAL-TIME LOGGING
import numpy as np
import pandas as pd
import requests
import csv
import os
import json 
from datetime import datetime
import urllib3
from dataclasses import dataclass
from typing import List, Dict, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import gspread
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound

import google.auth
import gspread.auth

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Custom Exceptions ---
class QuotaExceededError(Exception):
    """Raised when the AccuWeather API rate limit is hit."""
    pass

# --- Configuration ---
@dataclass
class Config:
    API_KEYS: List[str] = None
    BASE_URL: str = "http://dataservice.accuweather.com"
    CITIES_CSV_PATH: str = r"weather_locations_with_keys.csv"

    SHEET_URL: str = "https://docs.google.com/spreadsheets/d/10j8OyNxJg8McjEThKmyf1VQkfUZeLmTdAQqwopt3EHo/edit?gid=39425512#gid=39425512&fvid=2018402946"

    OUTPUT_GSHEET_NAME: str = "Weather Forecast"
    DAILY_SHEET_NAME: str = "5days_raw"
    HOURLY_SHEET_NAME: str = "12hrs_raw"
    MAX_RETRIES: int = 1 # 🎯 Reduced retries to fail faster on quota limits
    TIMEOUT: int = 10

    def __post_init__(self):
        key = os.environ.get('ACCUWEATHER_API_KEY')
        if not key:
             key = "zpka_c11eefdc3da04b2497156acbd0f7871d_fc4832ba"
        self.API_KEYS = [key]

    @staticmethod
    def load_cities() -> List[Dict]:
        cities = []
        csv_path = Config.CITIES_CSV_PATH
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as file:
                reader = csv.DictReader(file)
                cities = [row for row in reader]
            print(f"✅ Successfully loaded {len(cities)} locations from: {csv_path}")
            sys.stdout.flush() # 🎯 Force GitHub to print immediately
            return cities
        except FileNotFoundError:
            print(f"🛑 ERROR: Cities CSV file not found at {csv_path}.")
            sys.stdout.flush()
            return []
        except Exception as e:
            print(f"🛑 ERROR: Error reading CSV file: {e}")
            sys.stdout.flush()
            return []

# --- API Client ---
class AccuWeatherClient:
    def __init__(self, config: Config):
        self.config = config
        self.key_index = 0
        self.session = self._create_session()
        self.delay_between_calls = 1.5 # 🎯 Increased delay to respect 1 call/sec limit

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=self.config.MAX_RETRIES,
            backoff_factor=1,
            # 🎯 Removed 503 from auto-retry. 503 usually means Quota Exceeded for AccuWeather.
            status_forcelist=[500, 502, 504] 
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _make_request(self, url: str, params: Dict = None) -> Optional[Dict]:
        if params is None:
            params = {}
        while self.key_index < len(self.config.API_KEYS):
            params['apikey'] = self.config.API_KEYS[self.key_index]
            try:
                response = self.session.get(url, params=params, verify=False, timeout=self.config.TIMEOUT)
                
                # 🎯 The Circuit Breaker logic
                if response.status_code in [401, 403, 503]:
                    print(f"🛑 CRITICAL: API returned status {response.status_code}. Quota is likely exhausted.")
                    sys.stdout.flush()
                    raise QuotaExceededError("AccuWeather Daily Rate Limit Exceeded.")
                    
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.HTTPError as e:
                print(f"🛑 ERROR: HTTP error occurred: {e}")
                sys.stdout.flush()
                return None
            except requests.exceptions.RequestException as e:
                print(f"🛑 ERROR: Network error occurred: {e}")
                sys.stdout.flush()
                return None
                
        return None

    def get_daily_forecast(self, display_name: str, location_key: str) -> Optional[Dict]:
        url = f"{self.config.BASE_URL}/forecasts/v1/daily/5day/{location_key}"
        return self._make_request(url, {'metric': True, 'details': True})

    def get_hourly_forecast(self, display_name: str, location_key: str) -> Optional[List]:
        url = f"{self.config.BASE_URL}/forecasts/v1/hourly/12hour/{location_key}"
        return self._make_request(url, {'metric': True, 'details': True})

    def fetch_city_data(self, city_config: Dict) -> Optional[Dict]:
        city = city_config.get('City', 'Unknown City')
        zone = city_config.get('zone', 'Unknown Zone')
        loc_key = city_config.get('location_key')

        if not loc_key:
            print(f"⚠️ WARNING: Location key not found for {zone} {city}. Skipping.")
            sys.stdout.flush()
            return None

        city_details = {'city': city, 'zone': zone}
        display_name = f"{zone} {city}".strip()
        
        print(f"🔄 Fetching data for: {display_name} (Key: {loc_key})")
        sys.stdout.flush()

        time.sleep(self.delay_between_calls)
        daily_forecast = self.get_daily_forecast(display_name, loc_key)

        time.sleep(self.delay_between_calls)
        hourly_forecast = self.get_hourly_forecast(display_name, loc_key)

        # Ensure both forecasts were fetched successfully before returning
        if daily_forecast is None or hourly_forecast is None:
             return None

        return {'city_details': city_details, 'daily_forecast': daily_forecast, 'hourly_forecast': hourly_forecast}


# --- Data Processing Functions ---
def process_daily_data(city_details: Dict, api_response: Dict) -> List[Dict]:
    rows = []
    if not api_response or 'DailyForecasts' not in api_response: return rows
    for day in api_response['DailyForecasts']:
        try:
            formatted_date = datetime.fromisoformat(day.get('Date', '')).strftime('%m/%d/%Y')
        except (ValueError, TypeError):
            formatted_date = day.get('Date', '')[:10]
        base_data = {
            'City': city_details['city'], 'Zone': city_details['zone'], 'Date': formatted_date,
            'Temp_Min_C': day.get('Temperature', {}).get('Minimum', {}).get('Value'),
            'Temp_Max_C': day.get('Temperature', {}).get('Maximum', {}).get('Value'),
            'RealFeel_Temp_Min_C': day.get('RealFeelTemperature', {}).get('Minimum', {}).get('Value'),
            'RealFeel_Temp_Max_C': day.get('RealFeelTemperature', {}).get('Maximum', {}).get('Value'),
            'RealFeel_Temp_Shade_Min_C': day.get('RealFeelTemperatureShade', {}).get('Minimum', {}).get('Value'),
            'RealFeel_Temp_Shade_Max_C': day.get('RealFeelTemperatureShade', {}).get('Maximum', {}).get('Value'),
        }
        for period_name, period_data in [('Day', day.get('Day')), ('Night', day.get('Night'))]:
            if not period_data: continue
            row = base_data.copy()
            row.update({
                'Period': period_name, 'IconPhrase': period_data.get('IconPhrase'),
                'ShortPhrase': period_data.get('ShortPhrase'), 'LongPhrase': period_data.get('LongPhrase'),
                'PrecipitationProbability': period_data.get('PrecipitationProbability'),
                'ThunderstormProbability': period_data.get('ThunderstormProbability'),
                'RainProbability': period_data.get('RainProbability'),
                'Wind_Speed_kmh': period_data.get('Wind', {}).get('Speed', {}).get('Value'),
                'Wind_Direction_English': period_data.get('Wind', {}).get('Direction', {}).get('English'),
                'WindGust_Speed_kmh': period_data.get('WindGust', {}).get('Speed', {}).get('Value'),
                'TotalLiquid_mm': period_data.get('TotalLiquid', {}).get('Value'),
                'Rain_mm': period_data.get('Rain', {}).get('Value'),
                'HoursOfPrecipitation': period_data.get('HoursOfPrecipitation'),
                'HoursOfRain': period_data.get('HoursOfRain'),
                'Evapotranspiration_mm': period_data.get('Evapotranspiration', {}).get('Value'),
                'Avg_RelativeHumidity': period_data.get('RelativeHumidity', {}).get('Average'),
            })
            rows.append(row)
    return rows

def process_hourly_data(city_details: Dict, api_response: List) -> List[Dict]:
    rows = []
    if not api_response or not isinstance(api_response, list): return rows
    for hour in api_response:
        dt_object = datetime.fromisoformat(hour.get('DateTime'))
        rows.append({
            'City': city_details['city'], 'Zone': city_details['zone'],
            'Date': dt_object.strftime('%m/%d/%Y'), 'Hour': dt_object.hour,
            'IconPhrase': hour.get('IconPhrase'), 'HasPrecipitation': hour.get('HasPrecipitation'),
            'Temperature_Value': hour.get('Temperature', {}).get('Value'),
            'RealFeelTemperature_Value': hour.get('RealFeelTemperature', {}).get('Value'),
            'RealFeelTemperatureShade_Value': hour.get('RealFeelTemperatureShade', {}).get('Value'),
            'Wind_Speed_Value': hour.get('Wind', {}).get('Speed', {}).get('Value'),
            'Wind_Direction_Degrees': hour.get('Wind', {}).get('Direction', {}).get('Degrees'),
            'Wind_Direction_English': hour.get('Wind', {}).get('Direction', {}).get('English'),
            'WindGust_Speed_Value': hour.get('WindGust', {}).get('Speed', {}).get('Value'),
            'RelativeHumidity': hour.get('RelativeHumidity'), 'Visibility_Value': hour.get('Visibility', {}).get('Value'),
            'PrecipitationProbability': hour.get('PrecipitationProbability'),
            'ThunderstormProbability': hour.get('ThunderstormProbability'),
            'RainProbability': hour.get('RainProbability'), 'TotalLiquid_Value': hour.get('TotalLiquid', {}).get('Value'),
            'Rain_Value': hour.get('Rain', {}).get('Value'), 'Evapotranspiration_Value': hour.get('Evapotranspiration', {}).get('Value'),
            'SolarIrradiance_Value': hour.get('SolarIrradiance', {}).get('Value'),
        })
    return rows

# --- Google Sheets Manager ---
class GoogleSheetsManager:
    def __init__(self, config: Config):
        try:
            creds_json = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
            if not creds_json:
                creds, _ = google.auth.default()
                self.gc = gspread.authorize(creds)
                print("✅ gspread authorized using default local credentials.")
            else:
                creds_dict = json.loads(creds_json)
                if "private_key" in creds_dict:
                    creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
                self.gc = gspread.service_account_from_dict(creds_dict)
                print("✅ gspread authorized using Service Account from environment variable.")
            sys.stdout.flush()
        except Exception as e:
            raise Exception(f"🛑 Failed to perform Google Sheets authentication: {e}")

        self.config = config
        self.spreadsheet = self._open_existing_spreadsheet()

    def _open_existing_spreadsheet(self):
        sheet_url = self.config.SHEET_URL
        sheet_name = self.config.OUTPUT_GSHEET_NAME

        if sheet_url and sheet_url != "YOUR_EXISTING_GOOGLE_SHEET_URL_HERE":
            try:
                print(f"🔗 Opening existing Google Sheet using URL...")
                sys.stdout.flush()
                return self.gc.open_by_url(sheet_url)
            except Exception as e:
                print(f"⚠️ ERROR: Failed to open sheet by URL/ID. Trying by name. Error: {e}")
                sys.stdout.flush()

        try:
            print(f"🔗 Attempting to open Google Sheet by name: {sheet_name}")
            sys.stdout.flush()
            return self.gc.open(sheet_name)
        except SpreadsheetNotFound:
            print(f"➕ Spreadsheet '{sheet_name}' not found. Creating a new one...")
            sys.stdout.flush()
            return self.gc.create(sheet_name)
        except Exception as e:
            raise Exception(f"🛑 Failed to access or create spreadsheet: {e}. Check permissions!")

    def _get_or_add_worksheet(self, sheet_name: str):
        try:
            return self.spreadsheet.worksheet(sheet_name)
        except WorksheetNotFound:
            print(f"➕ Adding new worksheet: {sheet_name}")
            sys.stdout.flush()
            return self.spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=1)
        except Exception as e:
            print(f"🛑 ERROR: Failed to access or create worksheet {sheet_name}: {e}")
            sys.stdout.flush()
            raise
    
    def _clean_df_for_gsheet(self, df: pd.DataFrame) -> pd.DataFrame:
        df_cleaned = df.replace([np.inf, -np.inf], np.nan)
        return df_cleaned.where(pd.notnull(df_cleaned), None)
    
    def write_and_deduplicate(self, sheet_name: str, data: List[Dict]):
        if not data:
            print(f"⚠️ WARNING: No data provided for sheet: {sheet_name}. Skipping.")
            sys.stdout.flush()
            return

        df = pd.DataFrame(data)
        df['Run_Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            wks = self._get_or_add_worksheet(sheet_name)
            existing_data = wks.get_all_records()

            if existing_data:
                existing_df = pd.DataFrame(existing_data)
                
                if 'Date' in existing_df.columns:
                    try:
                        existing_df['Date'] = pd.to_datetime(existing_df['Date'], errors='coerce')
                        existing_df['Date'] = existing_df['Date'].dt.strftime('%m/%d/%Y')
                    except Exception as e:
                        print(f"⚠️ WARNING: Date standardization failed on existing data: {e}. Deduplication may be impaired.")
                        
                key_cols = ['City', 'Zone', 'Date', 'Period'] 
                for col in key_cols:
                    if col in existing_df.columns:
                        existing_df[col] = existing_df[col].astype(str).str.strip()
                    if col in df.columns:
                        df[col] = df[col].astype(str).str.strip()

                combined_df = pd.concat([existing_df, df], ignore_index=True, sort=False)
            else:
                combined_df = df

        except Exception as e:
            print(f"⚠️ WARNING: Could not read existing data from {sheet_name}. Proceeding with new data only. Error: {e}")
            combined_df = df

        combined_df = self._clean_df_for_gsheet(combined_df)

        subset_cols = ['City', 'Zone', 'Date', 'Period']
        final_df = combined_df.drop_duplicates(subset=subset_cols, keep='last')
        final_df = self._clean_df_for_gsheet(final_df) 

        data_to_write = [final_df.columns.values.tolist()] + final_df.values.tolist()

        try:
            wks.clear()
            wks.update(data_to_write, value_input_option='USER_ENTERED')
            print(f"✅ Data successfully written and deduplicated (OVERWRITE) to sheet '{sheet_name}'.")
            sys.stdout.flush()
        except Exception as e:
            print(f"🛑 ERROR: Failed to write data to Google Sheet: {e}")
            sys.stdout.flush()

    def append_data_log(self, sheet_name: str, data: List[Dict]):
        if not data:
            print(f"⚠️ WARNING: No data provided to append to sheet: {sheet_name}. Skipping.")
            sys.stdout.flush()
            return

        df = pd.DataFrame(data)
        df['Run_Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df_final = self._clean_df_for_gsheet(df)

        wks = self._get_or_add_worksheet(sheet_name)

        try:
            existing_values = wks.get_all_values()
            is_empty = not existing_values or (len(existing_values) == 1 and all(v == '' for v in existing_values[0]))

            if is_empty:
                data_to_write = [df_final.columns.values.tolist()] + df_final.values.tolist()
                wks.update(data_to_write, value_input_option='USER_ENTERED')
                print(f"✅ Sheet '{sheet_name}' was empty. Wrote initial data (with headers).")
            else:
                data_to_append = df_final.values.tolist()
                wks.append_rows(data_to_append, value_input_option='USER_ENTERED')
                print(f"✅ Successfully appended {len(data_to_append)} rows to sheet '{sheet_name}'.")
            sys.stdout.flush()

        except Exception as e:
            print(f"🛑 ERROR: Failed to append data to Google Sheet '{sheet_name}': {e}")
            sys.stdout.flush()

# --- Main Execution ---
def run_weather_processing(config: Config):
    client = AccuWeatherClient(config)
    cities = Config.load_cities()
    if not cities:
        print("🛑 ERROR: No cities loaded. Exiting.")
        return [], []

    all_daily_data, all_hourly_data = [], []

    for city in cities:
        try:
            result = client.fetch_city_data(city)
            if result:
                all_daily_data.extend(process_daily_data(result['city_details'], result['daily_forecast']))
                all_hourly_data.extend(process_hourly_data(result['city_details'], result['hourly_forecast']))
        except QuotaExceededError as e:
            print(f"\n🚨 ABORTING FETCH LOOP: {e}")
            print(f"💾 Saving the {len(all_daily_data)} rows of data collected so far...")
            sys.stdout.flush()
            break # Break out of the loop completely, but retain the data we already fetched
        except Exception as e:
            print(f"🛑 ERROR processing {city.get('City', 'Unknown City')} ({city.get('zone', 'Unknown Zone')}): {e}")
            sys.stdout.flush()

    return all_daily_data, all_hourly_data

def save_to_google_sheets(config: Config, daily_data: List[Dict], hourly_data: List[Dict]):
    if not daily_data and not hourly_data:
        print("⚠️ WARNING: No data provided to save to Google Sheets. Exiting save phase.")
        sys.stdout.flush()
        return

    print(f"\n--- 💾 Saving Data to Google Sheet ---")
    sys.stdout.flush()
    try:
        gs_manager = GoogleSheetsManager(config)

        print(f"\n📝 Processing Daily Forecast for '{config.DAILY_SHEET_NAME}' (Overwrite/Deduplicate)")
        sys.stdout.flush()
        gs_manager.write_and_deduplicate(config.DAILY_SHEET_NAME, daily_data)

        print(f"\n📝 Processing Hourly Forecast for '{config.HOURLY_SHEET_NAME}' (Append Log)")
        sys.stdout.flush()
        gs_manager.append_data_log(config.HOURLY_SHEET_NAME, hourly_data)

    except Exception as e:
        print(f"🛑 ERROR: An unexpected error occurred during Google Sheets saving: {e}")
        sys.stdout.flush()

def main():
    print("--- 🚀 Starting Weather Data Fetch Process ---")
    sys.stdout.flush()
    config = Config()

    try:
        daily_data, hourly_data = run_weather_processing(config)
        save_to_google_sheets(config, daily_data, hourly_data)
        print("--- 🎉 Process Finished! ---")
        sys.stdout.flush()
    except Exception as e:
        print(f"🛑 CRITICAL ERROR: A fatal error occurred in main execution loop: {e}")
        sys.stdout.flush()

if __name__ == "__main__":
    main()
