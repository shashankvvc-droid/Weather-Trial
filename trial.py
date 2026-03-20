import os
import json
import gspread
from gspread.exceptions import SpreadsheetNotFound, APIError
from google.auth.exceptions import DefaultCredentialsError, RefreshError

# The specific sheet URL from your config
SHEET_URL = "https://docs.google.com/spreadsheets/d/17NOMeO6L2IyRMk-ksiFMzu72wx5YJxwvG3A_9VznEWM/edit"

def test_google_sheets_auth():
    print("--- Starting Google Sheets Authentication Diagnostic ---")
    
    # SCENARIO A: Environment variable is missing entirely
    creds_json = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
    if not creds_json:
        print("🛑 LOG A: Missing Secret. The 'GSPREAD_SERVICE_ACCOUNT' environment variable is empty or not loaded.")
        return

    # SCENARIO B: The secret is not valid JSON
    try:
        creds_dict = json.loads(creds_json)
        print("✅ JSON parsed successfully.")
    except json.JSONDecodeError as e:
        print(f"🛑 LOG B: Invalid JSON format. GitHub might have injected extra quotes or stripped characters. Details: {e}")
        return

    # Check if 'private_key' actually exists in the dict
    if "private_key" not in creds_dict:
        print("🛑 LOG C: Missing Key. The parsed JSON does not contain the 'private_key' field.")
        return

    # SCENARIO C (The Newline Fix): Apply the fix and log it
    original_key = creds_dict["private_key"]
    if "\\n" in original_key:
        print("⚠️ NOTICE: Escaped newlines ('\\n') detected in private_key. Applying the replace fix...")
        creds_dict["private_key"] = original_key.replace("\\n", "\n")
    elif "\n" not in original_key:
        print("⚠️ NOTICE: No newlines detected at all. The key might be formatted as one long, invalid string.")

    # SCENARIO D: Authentication attempt
    try:
        gc = gspread.service_account_from_dict(creds_dict)
        print("✅ gspread client initialized.")
    except Exception as e:
        print(f"🛑 LOG D: Initialization failure. gspread could not process the credentials dictionary. Details: {e}")
        return

    # SCENARIO E & F: Network request to actually open the sheet
    try:
        print("Attempting to open the Google Sheet by URL...")
        sheet = gc.open_by_url(SHEET_URL)
        print(f"🎉 SUCCESS! Successfully connected to Google Sheets and opened: '{sheet.title}'")
        
    except RefreshError as e:
        # This catches the specific "invalid_grant: Invalid JWT Signature" error
        print(f"🛑 LOG E: Invalid JWT Signature / Revoked Key. The crypto signature failed.")
        print(f"   Details: {e}")
        print("   -> Conclusion: If you applied the newline fix and still see this, the key has been DELETED or REVOKED in Google Cloud.")
        
    except APIError as e:
        # This usually catches 403 Permission Denied
        if e.response.status_code == 403:
            print(f"🛑 LOG F: Permission Denied (403). The Service Account authenticated successfully, but its email address has not been shared as an 'Editor' on the Google Sheet itself.")
            print(f"   -> Make sure you shared the sheet with: {creds_dict.get('client_email')}")
        else:
            print(f"🛑 LOG G: Google API Error. Details: {e}")
            
    except SpreadsheetNotFound:
        print("🛑 LOG H: Spreadsheet Not Found (404). Authentication succeeded, but the URL is incorrect or the Service Account has absolutely no access to see it.")
        
    except Exception as e:
        print(f"🛑 LOG I: An unexpected error occurred during the fetch. Details: {e}")

if __name__ == "__main__":
    test_google_sheets_auth()
