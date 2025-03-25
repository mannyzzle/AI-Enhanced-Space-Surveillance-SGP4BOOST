import os
import re
import datetime
import glob
import time
import concurrent.futures
from math import sqrt
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, Boolean, MetaData, Table, UniqueConstraint
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, insert
from sqlalchemy.orm import sessionmaker
import psycopg2
from dotenv import load_dotenv
from sgp4.api import Satrec
from tqdm import tqdm

# Load environment variables
load_dotenv()

# Fetch environment variables
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")  # Ensure it's a string

# Build connection string for SQLAlchemy
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Define the Starlink TLE table with a unique constraint on (norad_id, epoch)
starlink_tle = Table(
    'starlink_tle', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('norad_id', Integer, index=True),
    Column('epoch', DateTime),
    Column('tle_line1', String),
    Column('tle_line2', String),
    Column('altitude_km', DOUBLE_PRECISION, nullable=True),
    Column('x', DOUBLE_PRECISION, nullable=True),
    Column('y', DOUBLE_PRECISION, nullable=True),
    Column('z', DOUBLE_PRECISION, nullable=True),
    Column('velocity_kms', DOUBLE_PRECISION, nullable=True),
    Column('vx', DOUBLE_PRECISION, nullable=True),
    Column('vy', DOUBLE_PRECISION, nullable=True),
    Column('vz', DOUBLE_PRECISION, nullable=True),
    Column('is_active', Boolean, default=True),
    UniqueConstraint('norad_id', 'epoch', name='uix_norad_epoch')
)

# Uncomment the next line if you want to drop the existing table before recreating it.
# metadata.drop_all(engine, [starlink_tle])
metadata.create_all(engine)

# Precompile the NORAD ID regex to improve performance
NORAD_REGEX = re.compile(r"1 (\d+)")

def jday_to_datetime(jd, fr):
    """
    Convert Julian Date (jd + fraction) to a UTC datetime.
    """
    jd_full = jd + fr
    JD_UNIX_EPOCH = 2440587.5  # Julian Date for 1970-01-01
    timestamp = (jd_full - JD_UNIX_EPOCH) * 86400.0
    return datetime.datetime.utcfromtimestamp(timestamp)

def fetch_existing_tles():
    """
    Fetch existing (norad_id, epoch) pairs from the database.
    Updated for SQLAlchemy 2.0+ by passing columns as positional arguments.
    """
    with engine.begin() as conn:
        result = conn.execute(
            starlink_tle.select().with_only_columns(starlink_tle.c.norad_id, starlink_tle.c.epoch)
        )
        return set((row[0], row[1]) for row in result)

def parse_tle_file(file_path, existing_tles=None):
    """
    Reads a TLE .txt file and yields parsed rows as dictionaries.
    Assumes the file contains alternating TLE line 1 and line 2.
    If propagation fails, derived fields are stored as None.
    Skips computation if (norad_id, epoch) is already in existing_tles.
    """
    with open(file_path, 'r') as f:
        # Using splitlines() for slightly better performance on large files
        lines = f.read().splitlines()

    if len(lines) % 2 != 0:
        print(f"Warning: {file_path} has an odd number of lines. The last line will be ignored.")

    for i in range(0, len(lines) - 1, 2):
        line1 = lines[i].strip()
        line2 = lines[i+1].strip()
        try:
            # Extract NORAD ID early
            match = NORAD_REGEX.search(line1)
            if not match:
                print(f"Warning: Could not extract NORAD ID from line: {line1}")
                continue
            norad_id = int(match.group(1))
            
            # Create satellite object and compute epoch
            sat = Satrec.twoline2rv(line1, line2)
            jd = sat.jdsatepoch
            fr = sat.jdsatepochF
            epoch = jday_to_datetime(jd, fr)
            
            # Skip if TLE already exists in DB
            if existing_tles and (norad_id, epoch) in existing_tles:
                continue

            error_code, position, velocity = sat.sgp4(jd, fr)
            if error_code != 0:
                altitude = None
                pos_components = [None, None, None]
                vel_components = [None, None, None]
                velocity_magnitude = None
                # Log decayed TLE if desired
                print(f"Info: TLE starting with {line1} flagged as decayed (error code {error_code}).")
            else:
                x, y, z = position
                pos_components = [round(x, 4), round(y, 4), round(z, 4)]
                r_norm = sqrt(x**2 + y**2 + z**2)
                altitude = r_norm - 6371  # Earth's radius in km
                vx, vy, vz = velocity
                vel_components = [round(vx, 4), round(vy, 4), round(vz, 4)]
                velocity_magnitude = round(sqrt(vx**2 + vy**2 + vz**2), 4)

            yield {
                "norad_id": norad_id,
                "epoch": epoch,
                "tle_line1": line1,
                "tle_line2": line2,
                "altitude_km": round(altitude, 2) if altitude is not None else None,
                "x": pos_components[0],
                "y": pos_components[1],
                "z": pos_components[2],
                "velocity_kms": velocity_magnitude,
                "vx": vel_components[0],
                "vy": vel_components[1],
                "vz": vel_components[2]
            }
        except Exception as e:
            print(f"Error parsing TLE pair starting at line {i+1} in {file_path}: {e}")
            continue

def chunked_insert(records, chunk_size=100, max_retries=3, retry_delay=5):
    """
    Insert the records in chunks using ON CONFLICT ON CONSTRAINT to skip duplicates.
    If a chunk fails, it retries up to max_retries with a delay.
    """
    for i in tqdm(range(0, len(records), chunk_size), desc="Inserting chunks"):
        chunk = records[i:i+chunk_size]
        stmt = insert(starlink_tle).values(chunk)
        # Use ON CONFLICT on the unique constraint 'uix_norad_epoch'
        stmt = stmt.on_conflict_do_nothing(constraint='uix_norad_epoch')
        attempts = 0
        while attempts < max_retries:
            try:
                with engine.begin() as conn:
                    conn.execute(stmt)
                break
            except Exception as e:
                attempts += 1
                print(f"Error inserting chunk {i} to {i+len(chunk)-1} (attempt {attempts}): {e}")
                time.sleep(retry_delay)
        else:
            print(f"Failed to insert chunk {i} to {i+len(chunk)-1} after {max_retries} attempts.")

def insert_tle_file_to_db(file_path, is_active=True, existing_tles=None):
    """
    Parses a TLE file and inserts its entries into the starlink_tle table using chunked inserts.
    Adds the is_active flag based on which folder the file came from.
    """
    parsed_tles = list(parse_tle_file(file_path, existing_tles))
    if not parsed_tles:
        print(f"No valid TLE records found in {file_path}")
        return
    for record in parsed_tles:
        record["is_active"] = is_active
    chunked_insert(parsed_tles, chunk_size=100)
    print(f"✅ Inserted {len(parsed_tles)} TLE records from {os.path.basename(file_path)} (is_active={is_active})")

def batch_insert_tle_from_directory(directory_path, is_active=True, max_workers=4, existing_tles=None):
    """
    Iterates over every .txt file in the given directory and inserts the TLE data into the database.
    Files are processed concurrently using a thread pool.
    """
    txt_files = glob.glob(os.path.join(directory_path, "*.txt"))
    if not txt_files:
        print(f"No .txt files found in {directory_path}")
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Schedule all file insertions concurrently
        futures = {
            executor.submit(insert_tle_file_to_db, file_path, is_active, existing_tles): file_path 
            for file_path in txt_files
        }
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Processing files"):
            try:
                future.result()
            except Exception as exc:
                print(f"File {futures[future]} generated an exception: {exc}")

if __name__ == "__main__":
    # Paths to the active and inactive directories
    active_dir = "tle_data/starlink_active"
    inactive_dir = "tle_data/starlink_inactive"

    print("🔍 Fetching existing TLE entries from DB...")
    existing_tles = fetch_existing_tles()

    print("Processing active Starlink TLE files...")
    batch_insert_tle_from_directory(active_dir, is_active=True, max_workers=10, existing_tles=existing_tles)

    print("Processing inactive Starlink TLE files...")
    batch_insert_tle_from_directory(inactive_dir, is_active=False, max_workers=10, existing_tles=existing_tles)
