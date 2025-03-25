import os
import glob
import io
import pandas as pd
import psycopg2
import concurrent.futures
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

# Fetch environment variables for DB connection
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")  # Ensure it's a string

# Create SQLAlchemy engine for schema/table inspection
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

def get_psycopg2_connection():
    """Create a psycopg2 connection for COPY operations."""
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

# Per your OMNI spec:
col_widths = [
    4, 4, 3, 3, 4, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    7, 9, 6, 7, 7, 6, 8, 8, 8, 8, 8, 8, 6, 6, 6, 6, 6, 6, 6
]
col_names = [
    "Year", "Day", "Hour", "Minute", "Percent_Interpolation",
    "Timeshift", "RMS_Timeshift", "Time_btwn_obs_sec", "Field_Mag_Avg_nT",
    "BX_GSE_GSM_nT", "BY_GSE_nT", "BZ_GSE_nT", "BY_GSM_nT", "BZ_GSM_nT",
    "Speed_km_s", "Vx_km_s", "Vy_km_s", "Vz_km_s", "Proton_Density",
    "Proton_Temp_K", "Flow_Pressure_nPa", "Electric_Field_mV_m",
    "Plasma_beta", "Alfven_Mach_Number", "SC_Xgse_Re", "SC_Ygse_Re",
    "SC_Zgse_Re", "BSN_Xgse_Re", "BSN_Ygse_Re", "BSN_Zgse_Re",
    "AE_index_nT", "AL_index_nT", "AU_index_nT", "SYM_D_nT",
    "SYM_H_nT", "ASY_D_nT", "ASY_H_nT"
]

# Validate matching lengths
if len(col_widths) != len(col_names):
    raise ValueError("Mismatch: col_widths and col_names length differ.")

na_values = [
    "9999", " 9999",
    "99999", " 99999",
    "99999.9", " 99999.9",
    "9999.99", " 9999.99",
    "999.9", " 999.9",
    "9999999.", " 9999999.",
    "9999999", " 9999999"
]

def fetch_existing_epochs():
    """
    Return a set of epoch values already in 'omni_data'.
    """
    insp = inspect(engine)
    if not insp.has_table("omni_data"):
        return set()
    df_existing = pd.read_sql("SELECT \"epoch\" FROM \"omni_data\"", engine, parse_dates=["epoch"])
    return set(df_existing["epoch"])

def parse_single_file(file_path, skiprows=20):
    """Parse one OMNI file into a DataFrame."""
    temp_df = pd.read_fwf(
        file_path,
        widths=col_widths,
        names=col_names,
        header=None,
        skiprows=skiprows,
        na_values=na_values,
        keep_default_na=True
    )
    # Construct the epoch column
    temp_df["epoch"] = pd.to_datetime(
        temp_df["Year"].astype(str) + " " +
        temp_df["Day"].astype(str) + " " +
        temp_df["Hour"].astype(str) + ":" +
        temp_df["Minute"].astype(str),
        format="%Y %j %H:%M",
        errors="coerce"
    )
    temp_df.dropna(subset=["epoch"], inplace=True)
    return temp_df

def load_omni_data(omni_folder):
    """
    Parse all OMNI files concurrently, merge, deduplicate, and sort by epoch.
    """
    file_paths = glob.glob(os.path.join(omni_folder, "OMNI_*.lst"))
    if not file_paths:
        print(f"No OMNI .lst files found in {omni_folder}")
        return pd.DataFrame()

    df_list = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(parse_single_file, fp): fp for fp in file_paths}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Parsing files"):
            path = futures[future]
            try:
                df_part = future.result()
                df_list.append(df_part)
            except Exception as exc:
                print(f"Error parsing {path}: {exc}")

    if not df_list:
        return pd.DataFrame()

    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df.drop_duplicates(subset=["epoch"], inplace=True)
    
    # reorder columns: epoch first
    cols = ["epoch"] + [c for c in combined_df.columns if c != "epoch"]
    combined_df = combined_df[cols]
    combined_df.sort_values("epoch", inplace=True)
    return combined_df

def copy_using_psycopg2(df, table_name="omni_data"):
    """
    Bulk-load the DataFrame into PostgreSQL using psycopg2 COPY command,
    quoting all column identifiers to match the table's quoted uppercase columns.
    """
    # Convert the DataFrame to in-memory CSV
    output = io.StringIO()
    df.to_csv(output, sep=",", header=False, index=False)
    output.seek(0)

    # QUOTE each column name to match your DB's "Year", "Day" ...
    columns_str = ", ".join(f'"{col}"' for col in df.columns)

    with get_psycopg2_connection() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            copy_sql = f"""
                COPY "{table_name}" ({columns_str})
                FROM STDIN
                WITH (
                    FORMAT CSV,
                    DELIMITER ',',
                    NULL ''
                );
            """
            try:
                cur.copy_expert(copy_sql, output)
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e

def main():
    omni_folder = "SpaceWeather"
    print("Loading OMNI data concurrently from files...")
    combined_df = load_omni_data(omni_folder)
    
    if combined_df.empty:
        print("No data to insert.")
        return
    print(f"Total rows combined after duplicate removal: {len(combined_df)}")

    # Filter out epochs already in DB
    existing_epochs = fetch_existing_epochs()
    if existing_epochs:
        print(f"Found {len(existing_epochs)} existing epochs. Filtering new data...")

    new_df = combined_df[~combined_df["epoch"].isin(existing_epochs)]
    new_df.sort_values("epoch", inplace=True)
    print(f"Total new rows to insert: {len(new_df)}")
    
    if new_df.empty:
        print("No new data to insert.")
        return
    
    print("Performing fast bulk copy into PostgreSQL (with quoted column names)...")
    copy_using_psycopg2(new_df, table_name="omni_data")
    print("✅ OMNI data inserted into 'omni_data' table using COPY and quoted columns.")

if __name__ == "__main__":
    main()
