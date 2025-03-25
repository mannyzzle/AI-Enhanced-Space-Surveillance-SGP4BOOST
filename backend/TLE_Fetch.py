import os
import requests
import psycopg2
import time
import pickle
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
import requests
import sys
from requests.exceptions import ConnectionError, Timeout, RequestException
import random

# Load environment variables
load_dotenv()

# Space-Track credentials
SPACETRACK_USER = os.getenv("SPACETRACK_USER")
SPACETRACK_PASS = os.getenv("SPACETRACK_PASS")

# PostgreSQL credentials
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# File path for GpDat.csv
GP_DAT_FILE = "GpData.csv"

# Space-Track API URLs
LOGIN_URL = "https://www.space-track.org/ajaxauth/login"
TLE_HISTORY_URL = "https://www.space-track.org/basicspacedata/query/class/gp_history/NORAD_CAT_ID/{}/orderby/EPOCH asc/format/tle"

# Cookie file
COOKIE_FILE = "cookies.pkl"

# Directory for TLE files
TLE_DIR = "tle_data"
ORBIT_DIR = os.path.join(TLE_DIR, "iridium_active")
DEORBITED_DIR = os.path.join(TLE_DIR, "iridium_inactive")
os.makedirs(TLE_DIR, exist_ok=True)
os.makedirs(DEORBITED_DIR, exist_ok=True)

# Initialize session
session = requests.Session()

def save_cookies():
    """Saves cookies to a file."""
    with open(COOKIE_FILE, "wb") as f:
        pickle.dump(session.cookies, f)

def load_cookies():
    """Loads cookies from a file if available."""
    if os.path.exists(COOKIE_FILE):
        with open(COOKIE_FILE, "rb") as f:
            session.cookies.update(pickle.load(f))

def check_session_valid():
    """Tests if the saved session is still valid by pinging Space-Track."""
    test_url = "https://www.space-track.org/basicspacedata/query/class/satcat/limit/1"
    response = session.get(test_url)
    
    if response.status_code == 200:
        print("✅ Session is still valid.")
        return True
    else:
        print("❌ Session expired or invalid. Need to log in again.")
        return False

def login(force=False):
    """Logs in to Space-Track and saves session cookies. Forces login if needed."""
    if not force:
        print("🔐 Checking for saved cookies...")
        load_cookies()  # Try loading existing cookies

        # ✅ Test session before using cookies
        if check_session_valid():
            return  # ✅ If valid, no need to log in

    print("🔐 Logging in to Space-Track...")
    payload = {"identity": SPACETRACK_USER, "password": SPACETRACK_PASS}
    response = session.post(LOGIN_URL, data=payload)

    if response.status_code == 200:
        print("✅ Successfully logged in!")
        save_cookies()  # ✅ Save new cookies
    else:
        print("❌ Login failed:", response.text)
        sys.exit(1)  # 🚨 Stop script if login fails

def get_norad_ids():
    """Fetches Starlink NORAD numbers from PostgreSQL, sorted by launch date (oldest first)."""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT norad_number FROM satellites 
        WHERE name ILIKE 'IRIDIUM%' 
        ORDER BY launch_date ASC;
    """)
    active_norad_ids = {str(row[0]) for row in cursor.fetchall()}  # Convert to set for fast lookup
    cursor.close()
    conn.close()

    print(f"📡 Retrieved {len(active_norad_ids)}  NORAD IDs from database (ordered by launch date).")
    return active_norad_ids

def get_all_from_csv():
    """Fetches all specifide satellite NORAD numbers from GpDat.csv."""
    df = pd.read_csv(GP_DAT_FILE)
    satellite_df = df[df["OBJECT_NAME"].str.contains("IRIDIUM", na=False, case=False)]
    all_norad_ids = set(satellite_df["NORAD_CAT_ID"].astype(str))

    print(f"📄 Found {len(all_norad_ids)} Total Satellites in GpData.csv.")
    return all_norad_ids




def fetch_tle_data(norad_list, retry_attempts=5):
    """Fetches historical TLE data for a batch of NORADs, ensuring proper format handling.
       Handles connection errors gracefully and retries failed requests.
    """
    for attempt in range(retry_attempts):
        norad_str = ",".join(norad_list)
        tle_url = TLE_HISTORY_URL.format(norad_str)

        print(f"🌍 Attempt {attempt+1}: Fetching TLE history for NORADs {norad_str}...")
        print(f"🔍 API URL: {tle_url}")

        try:
            response = session.get(tle_url, timeout=30)  # ✅ Set a timeout to prevent hanging

            print(f"🔍 HTTP {response.status_code} - Response Length: {len(response.text)}")

            # 🚨 Handle API Throttling
            if response.status_code == 429 or "Too Many Requests" in response.text:
                print("🚨 API THROTTLED: Too many requests! Stopping process completely.")
                sys.exit(1)

            # 🚨 Handle No Content (Empty Response)
            if response.status_code == 204 or len(response.text.strip()) == 0:
                print(f"⚠️ No TLE data available for NORADs {norad_str}. Skipping.")
                return None

            # ❌ If session expired, force re-login
            if "alert-danger" in response.text or response.status_code in [401, 403]:
                print("❌ Session expired or query error. Re-authenticating...")
                login(force=True)
                response = session.get(tle_url)

            if response.status_code == 200 and len(response.text.strip()) > 0:
                print(f"✅ Successfully fetched TLE data for NORADs {norad_str}.")
                return response.text.strip()

        except (ConnectionError, Timeout) as e:
            print(f"❌ Connection error: {e}. Retrying in {5 + 2*attempt} seconds...")
            time.sleep(5 + 2*attempt)  # Increase wait time before retrying

        except RequestException as e:
            print(f"🚨 Unexpected request error: {e}. Stopping process.")
            sys.exit(1)

    print(f"❌ All retry attempts failed for NORADs {norad_str}. Skipping batch.")
    return None




def split_and_save_tle(tle_data, active_norads):
    """Processes raw TLE data and ensures each NORAD ID gets its full historical TLEs."""
    if not tle_data:
        print("⚠️ No valid TLE data to process.")
        return

    tle_lines = tle_data.strip().split("\n")
    tle_dict = {}

    for i in range(0, len(tle_lines), 2):
        if i + 1 < len(tle_lines):
            line1, line2 = tle_lines[i], tle_lines[i + 1]

            if not line1.startswith("1 ") or not line2.startswith("2 "):
                continue

            norad_id = line1.split()[1][:5]
            tle_dict.setdefault(norad_id, []).append(f"{line1}\n{line2}")

    for norad_id, tle_content in tle_dict.items():
        is_deorbited = norad_id not in active_norads
        folder = DEORBITED_DIR if is_deorbited else ORBIT_DIR
        filename = os.path.join(folder, f"tle_{norad_id}.txt")

        with open(filename, "w") as file:
            file.write("\n".join(tle_content) + "\n")

        print(f"📂 TLEs saved to {filename}")

# Run the pipeline
login()
active_norads = get_norad_ids()
all_norads = get_all_from_csv()

norad_list = sorted(all_norads)



BATCH_SIZE = 15
WAIT_TIME = 60  # Base wait time in seconds

with tqdm(total=len(norad_list), desc="📡 Downloading TLEs", unit=" batch") as pbar:
    for i in range(0, len(norad_list), BATCH_SIZE):
        batch = norad_list[i:i+BATCH_SIZE]

        if all(os.path.exists(os.path.join(ORBIT_DIR, f"tle_{norad}.txt")) or os.path.exists(os.path.join(DEORBITED_DIR, f"tle_{norad}.txt")) for norad in batch):
            pbar.update(BATCH_SIZE)
            continue

        tle_data = fetch_tle_data(batch)
        if tle_data:
            split_and_save_tle(tle_data, active_norads)

        # ✅ Add randomness to the delay to avoid API detection
        random_wait = WAIT_TIME + random.randint(-10, 30)  # Wait between 50 - 90 sec
        print(f"⏳ Waiting {random_wait} seconds before next batch...")
        time.sleep(random_wait)

        pbar.update(BATCH_SIZE)