import numpy as np
import pandas as pd
import requests
import csv
import os
import json # <-- INDUCED: For handling Service Account JSON
from datetime import datetime
import urllib3
from dataclasses import dataclass
from typing import List, Dict, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import gspread
# REMOVED: from google.colab import auth, drive
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound

# 🛠️ Imports for correct authentication
import google.auth
import gspread.auth

# --- Setup & Authentication Cleanup ---
# REMOVED: drive.mount('/content/drive', force_remount=True)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# REMOVED: Colab-specific authentication block. 
# Authentication is now handled within the GoogleSheetsManager using os.environ.

# --- Configuration ---
@dataclass
class Config:
    """Holds configuration for the application."""
    API_KEYS: List[str] = None
    BASE_URL: str = "http://dataservice.accuweather.com"
    # REMOVED '/content/drive' prefix since GitHub Actions won't mount Drive
    CITIES_CSV_PATH: str = r"weather_locations_with_keys.csv" # Assumes CSV is in repo root

    # 🚨 CHANGE: This is now a reference, not a hardcoded placeholder
    SHEET_URL: str = "https://docs.google.com/spreadsheets/d/10j8OyNxJg8McjEThKmyf1VQkfUZeLmTdAQqwopt3EHo/edit?gid=817252250#gid=817252250&fvid=2047135506"

    OUTPUT_GSHEET_NAME: str = "Weather Forecast Dashboard"
    DAILY_SHEET_NAME: str = "5days_raw"
    HOURLY_SHEET_NAME: str = "12hrs_raw"
    MAX_RETRIES: int = 3
    TIMEOUT: int = 10

    def __post_init__(self):
        """Initialize API keys using environment variables."""
        # --- INDUCED CHANGE: Use API Key from Environment Variable (GitHub Secret) ---
        key = os.environ.get('ACCUWEATHER_API_KEY')
        if not key:
             # Fallback to the hardcoded key if the environment variable is missing (e.g., for local testing)
             key = "zpka_f6a4714c10654ad29bd6b2d793b5b9f5_acf75188"

        self.API_KEYS = [key]

    @staticmethod
    def load_cities() -> List[Dict]:
        """Loads city data from the specified CSV file."""
        cities = []
        csv_path = Config.CITIES_CSV_PATH
        try:
            # Note: Path is relative to the script location when run by GitHub Actions
            with open(csv_path, 'r', encoding='utf-8-sig') as file:
                reader = csv.DictReader(file)
                cities = [row for row in reader]
            print(f"Successfully loaded cities from: {csv_path}")
            return cities
        except FileNotFoundError:
            print(f"ERROR: Cities CSV file not found at {csv_path}. Ensure it is in the repository root.")
            return []
        except Exception as e:
            print(f"ERROR: Error reading CSV file: {e}")
            return []

# --- API Client (No Change) ---
class AccuWeatherClient:
    """A client to interact with the AccuWeather API."""
    # ... (AccuWeatherClient class methods remain unchanged)

    def __init__(self, config: Config):
        self.config = config
        self.key_index = 0
        self.session = self._create_session()
        self.delay_between_calls = 1

    def _create_session(self) -> requests.Session:
        """Creates a requests session with retry logic."""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.config.MAX_RETRIES,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _make_request(self, url: str, params: Dict = None) -> Optional[Dict]:
        """Makes a request to the API, handling key rotation."""
        if params is None:
            params = {}
        while self.key_index < len(self.config.API_KEYS):
            params['apikey'] = self.config.API_KEYS[self.key_index]
            try:
                response = self.session.get(url, params=params, verify=False, timeout=self.config.TIMEOUT)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code in [401, 403, 503]:
                    print(f"WARNING: API Key #{self.key_index + 1} failed or is exhausted. Trying next...")
                    self.key_index += 1
                else:
                    print(f"ERROR: HTTP error occurred: {e}")
                    return None
            except requests.exceptions.RequestException as e:
                print(f"ERROR: Network error occurred: {e}")
                return None
        print("ERROR: All API keys have been exhausted.")
        return None

    def get_daily_forecast(self, city: str, location_key: str) -> Optional[Dict]:
        print(f"Fetching 5-day forecast for: {city} (Key: {location_key})")
        url = f"{self.config.BASE_URL}/forecasts/v1/daily/5day/{location_key}"
        return self._make_request(url, {'metric': True, 'details': True})

    def get_hourly_forecast(self, city: str, location_key: str) -> Optional[List]:
        print(f"Fetching 12-hour forecast for: {city} (Key: {location_key})")
        url = f"{self.config.BASE_URL}/forecasts/v1/hourly/12hour/{location_key}"
        return self._make_request(url, {'metric': True, 'details': True})

    def fetch_city_data(self, city_config: Dict) -> Optional[Dict]:
        try:
            city_name = city_config.get('name', 'Unknown City')
            loc_key = city_config.get('location_key')
            if not loc_key:
                print(f"WARNING: Location key not found in CSV for {city_name}. Skipping.")
                return None
            name_parts = city_name.split(" ", 1)
            zone, city = (name_parts[0], name_parts[1]) if len(name_parts) > 1 else (city_config.get('zone', ''), name_parts[0])
            city_details = {'city': city, 'zone': zone}

            time.sleep(self.delay_between_calls)
            daily_forecast = self.get_daily_forecast(city_name, loc_key)

            time.sleep(self.delay_between_calls)
            hourly_forecast = self.get_hourly_forecast(city_name, loc_key)

            return {'city_details': city_details, 'daily_forecast': daily_forecast, 'hourly_forecast': hourly_forecast}
        except Exception as e:
            print(f"ERROR: Error processing city {city_name}: {e}")
            return None


# --- Data Processing Functions (No Change) ---
def process_daily_data(city_details: Dict, api_response: Dict) -> List[Dict]:
    rows = []
    if not api_response or 'DailyForecasts' not in api_response: return rows
    for day in api_response['DailyForecasts']:
        try:
            # New data is consistently formatted as MM/DD/YYYY (e.g., 11/02/2025)
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
        # Using .hour will give 0 for midnight, 1 for 1 AM, etc.
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

# --- Google Sheets Manager (MODIFIED for GitHub Actions) ---
class GoogleSheetsManager:
    """Manages reading and writing data to an existing Google Sheet using URL."""
    def __init__(self, config: Config):
        
        # --- INDUCED CHANGE: Use Service Account for GitHub Actions ---
        try:
            # Load credentials from the environment variable (GitHub Secret)
            creds_json = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
            if not creds_json:
                # If run locally/not in GitHub Actions, try default Colab/Local auth
                creds, _ = google.auth.default()
                self.gc = gspread.authorize(creds)
                print("gspread authorized using default local credentials.")
            else:
                creds_dict = json.loads(creds_json)
                self.gc = gspread.service_account_from_dict(creds_dict)
                print("gspread authorized using Service Account from environment variable.")
                
        except Exception as e:
            # Critical failure in authentication
            raise Exception(f"Failed to perform Google Sheets authentication: {e}")

        self.config = config
        self.spreadsheet = self._open_existing_spreadsheet()

    def _open_existing_spreadsheet(self):
        """Tries to open the sheet using the provided URL/ID or name."""
        sheet_url = self.config.SHEET_URL
        sheet_name = self.config.OUTPUT_GSHEET_NAME

        # Check if a specific URL was provided
        if sheet_url and sheet_url != "YOUR_EXISTING_GOOGLE_SHEET_URL_HERE":
            try:
                print(f"Opening existing Google Sheet using URL...")
                return self.gc.open_by_url(sheet_url)
            except Exception as e:
                print(f"ERROR: Failed to open sheet by URL/ID. Trying by name. Error: {e}")

        # Fallback to opening by name (which was previously attempting to create)
        try:
            print(f"Attempting to open Google Sheet by name: {sheet_name}")
            return self.gc.open(sheet_name)
        except SpreadsheetNotFound:
             # If we can't find it by URL or Name, we must create it.
            print(f"Spreadsheet '{sheet_name}' not found. Creating a new one...")
            return self.gc.create(sheet_name)
        except Exception as e:
            raise Exception(f"Failed to access or create spreadsheet: {e}. Check permissions!")

    def _get_or_add_worksheet(self, sheet_name: str):
        """Gets a worksheet by name, or creates it if necessary."""
        try:
            return self.spreadsheet.worksheet(sheet_name)
        except WorksheetNotFound:
            print(f"Adding new worksheet: {sheet_name}")
            return self.spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=1)
        except Exception as e:
            print(f"ERROR: Failed to access or create worksheet {sheet_name}: {e}")
            raise
    
    # --- INDUCED CHANGE: CLEANING METHOD ---
    def _clean_df_for_gsheet(self, df: pd.DataFrame) -> pd.DataFrame:
        """Replaces NaN, Inf, and -Inf values with None for gspread compliance."""
        # Replace all inf/-inf with NaN
        df_cleaned = df.replace([np.inf, -np.inf], np.nan)
        
        # Replace all NaN with None (gspread/JSON compliant empty cell)
        return df_cleaned.where(pd.notnull(df_cleaned), None)
    
    # --- INDUCED CHANGE: CLEANING APPLIED IN write_and_deduplicate ---
    def write_and_deduplicate(self, sheet_name: str, data: List[Dict]):
        """
        Writes data to the 5-Day Forecast sheet, ensuring the LATEST forecast
        replacing the OLDER one for the same City/Date/Period (Deduplication).
        """
        if not data:
            print(f"WARNING: No data provided for sheet: {sheet_name}. Skipping.")
            return

        df = pd.DataFrame(data)
        df['Run_Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            wks = self._get_or_add_worksheet(sheet_name)
            existing_data = wks.get_all_records()

            if existing_data:
                existing_df = pd.DataFrame(existing_data)
                
                # 🎯 CRITICAL FIX: Standardize the Date format in the existing data
                if 'Date' in existing_df.columns:
                    try:
                        # 1. Convert the existing date strings (padded or unpadded, e.g., 11/2/2025) 
                        #    into consistent datetime objects.
                        existing_df['Date'] = pd.to_datetime(existing_df['Date'], errors='coerce')
                        
                        # 2. Convert ALL dates back into the standard padded 'MM/DD/YYYY' string format (e.g., 11/02/2025).
                        existing_df['Date'] = existing_df['Date'].dt.strftime('%m/%d/%Y')
                    except Exception as e:
                        print(f"WARNING: Date standardization failed on existing data: {e}. Deduplication may be impaired.")
                        
                # Ensure all key columns (including the standardized 'Date') are treated as stripped strings for matching
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
            print(f"WARNING: Could not read existing data from {sheet_name}. Proceeding with new data only. Error: {e}")
            combined_df = df

        # Apply the JSON-compliance fix before processing
        combined_df = self._clean_df_for_gsheet(combined_df)

        subset_cols = ['City', 'Zone', 'Date', 'Period']
        final_df = combined_df.drop_duplicates(subset=subset_cols, keep='last')

        # Final check (redundant but safe)
        final_df = self._clean_df_for_gsheet(final_df) 

        # Convert the cleaned DataFrame to the list-of-lists format
        data_to_write = [final_df.columns.values.tolist()] + final_df.values.tolist()

        try:
            wks.clear()
            wks.update(data_to_write, value_input_option='USER_ENTERED')
            print(f"Data successfully written and deduplicated (OVERWRITE) to sheet '{sheet_name}'.")
        except Exception as e:
            print(f"ERROR: Failed to write data to Google Sheet: {e}")

    # --- INDUCED CHANGE: CLEANING APPLIED IN append_data_log ---
    def append_data_log(self, sheet_name: str, data: List[Dict]):
        """
        Appends new data rows to the end of the 12-Hour Forecast sheet.
        """
        if not data:
            print(f"WARNING: No data provided to append to sheet: {sheet_name}. Skipping.")
            return

        df = pd.DataFrame(data)
        df['Run_Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Apply the JSON-compliance fix before appending
        df_final = self._clean_df_for_gsheet(df)

        wks = self._get_or_add_worksheet(sheet_name)

        try:
            existing_values = wks.get_all_values()

            is_empty = not existing_values or (len(existing_values) == 1 and all(v == '' for v in existing_values[0]))

            if is_empty:
                # Use df_final (cleaned)
                data_to_write = [df_final.columns.values.tolist()] + df_final.values.tolist()
                wks.update(data_to_write, value_input_option='USER_ENTERED')
                print(f"Sheet '{sheet_name}' was empty. Wrote initial data (with headers).")
            else:
                # Use df_final (cleaned)
                data_to_append = df_final.values.tolist()
                wks.append_rows(data_to_append, value_input_option='USER_ENTERED')
                print(f"Successfully appended {len(data_to_append)} rows to sheet '{sheet_name}'.")

        except Exception as e:
            print(f"ERROR: Failed to append data to Google Sheet '{sheet_name}': {e}")


# --- Main Execution ---
def run_weather_processing(config: Config):
    client = AccuWeatherClient(config)
    cities = Config.load_cities()
    if not cities:
        print("ERROR: No cities loaded. Exiting.")
        return [], []

    all_daily_data, all_hourly_data = [], []

    for city in cities:
        try:
            result = client.fetch_city_data(city)
            if result:
                all_daily_data.extend(process_daily_data(result['city_details'], result['daily_forecast']))
                all_hourly_data.extend(process_hourly_data(result['city_details'], result['hourly_forecast']))
        except Exception as e:
            print(f"CRITICAL ERROR processing {city.get('name', 'Unknown City')}: {e}")
        time.sleep(1)

    return all_daily_data, all_hourly_data

def save_to_google_sheets(config: Config, daily_data: List[Dict], hourly_data: List[Dict]):
    """Saves the data directly to Google Sheets using the appropriate methods."""
    if not daily_data and not hourly_data:
        print("WARNING: No data provided to save to Google Sheets.")
        return

    print(f"\n--- Saving Data to Google Sheet ---")
    try:
        gs_manager = GoogleSheetsManager(config)

        # Sheet 1: 5-Day Forecast -> OVERWRITE/DEDUPLICATION
        print(f"\nProcessing Daily Forecast for '{config.DAILY_SHEET_NAME}' (Overwrite/Deduplicate)")
        gs_manager.write_and_deduplicate(config.DAILY_SHEET_NAME, daily_data)

        # Sheet 2: 12-Hour Forecast -> PURE APPEND
        print(f"\nProcessing Hourly Forecast for '{config.HOURLY_SHEET_NAME}' (Append Log)")
        gs_manager.append_data_log(config.HOURLY_SHEET_NAME, hourly_data)

    except Exception as e:
        print(f"ERROR: An unexpected error occurred during Google Sheets saving: {e}")

def main():
    print("--- Starting Weather Data Fetch Process ---")
    config = Config()

    try:
        daily_data, hourly_data = run_weather_processing(config)
        save_to_google_sheets(config, daily_data, hourly_data)
        print("--- All tasks completed successfully! ---")
    except Exception as e:
        print(f"CRITICAL ERROR: A fatal error occurred in main execution loop: {e}")

if __name__ == "__main__":
    main()
