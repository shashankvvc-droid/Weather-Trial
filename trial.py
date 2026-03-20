import os
import json
import base64
import gspread
from gspread.exceptions import SpreadsheetNotFound, APIError
from google.auth.exceptions import DefaultCredentialsError, RefreshError

# The specific sheet URL from your config
SHEET_URL = "https://docs.google.com/spreadsheets/d/17NOMeO6L2IyRMk-ksiFMzu72wx5YJxwvG3A_9VznEWM/edit"

def test_google_sheets_auth():
    print("--- Starting Base64 Auth Diagnostic ---")
    
    # SCENARIO A: Environment variable is missing
    creds_b64 = os.environ.get('GSPREAD_SERVICE_ACCOUNT_B64')
    if not creds_b64:
        print("🛑 LOG A: Missing Secret. The 'GSPREAD_SERVICE_ACCOUNT_B64' environment variable is empty.")
        return

    # SCENARIO B: Base64 Decode failure
    try:
        # Decode the Base64 string back into standard UTF-8 JSON text
        creds_json = base64.b64decode(creds_b64).decode('utf-8')
        print("✅ Base64 string decoded successfully.")
    except Exception as e:
        print(f"🛑 LOG B: Base64 Decode Failed. The string might be incomplete. Details: {e}")
        return

    # SCENARIO C: JSON Parse failure
    try:
        creds_dict = json.loads(creds_json)
        print("✅ JSON parsed successfully from decoded string.")
    except json.JSONDecodeError as e:
        print(f"🛑 LOG C: Invalid JSON format after decoding. Details: {e}")
        return

    # SCENARIO D: Authentication Initialization
    try:
        gc = gspread.service_account_from_dict(creds_dict)
        print("✅ gspread client initialized.")
    except Exception as e:
        print(f"🛑 LOG D: Initialization failure. gspread could not process the credentials. Details: {e}")
        return

    # SCENARIO E: Network request to open the sheet
    try:
        print("Attempting to open the Google Sheet by URL...")
        sheet = gc.open_by_url(SHEET_URL)
        print(f"🎉 SUCCESS! Successfully connected to Google Sheets and opened: '{sheet.title}'")
        
    except RefreshError as e:
        print(f"🛑 LOG E: Invalid JWT Signature. Details: {e}")
    except APIError as e:
        if getattr(e.response, 'status_code', None) == 403:
            print(f"🛑 LOG F: Permission Denied (403). Make sure you shared the sheet with: {creds_dict.get('client_email')}")
        else:
            print(f"🛑 LOG G: Google API Error. Details: {e}")
    except SpreadsheetNotFound:
        print("🛑 LOG H: Spreadsheet Not Found (404).")
    except Exception as e:
        print(f"🛑 LOG I: An unexpected error occurred. Details: {e}")

if __name__ == "__main__":
    test_google_sheets_auth()
