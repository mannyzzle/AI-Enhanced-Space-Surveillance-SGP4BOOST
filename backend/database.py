from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import pandas as pd
import seaborn as sn
import psycopg2
# ✅ Load environment variables
load_dotenv()

# ✅ Fetch environment variables
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")  # Ensure it's a string



# 🔹 Database Connection Function
def get_db_connection():
    """Connect to PostgreSQL and return the connection object."""
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=5432
    )




def get_db_engine():
    try:
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        print("✅ Database connection established successfully!")
        return engine
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None
    
def fetch_satellite_data():
    """
    Fetch multiple TLEs per NORAD, sorted by time, for time-series training.
    """
    engine = get_db_engine()
    if not engine:
        return None  # Exit if connection fails

    query = """
        SELECT norad_number, epoch, semi_major_axis, eccentricity, inclination, 
               raan, arg_perigee, mean_motion, velocity, altitude_km, 
               latitude, longitude, x, y, z, vx, vy, vz
        FROM satellites
        WHERE epoch >= NOW() - INTERVAL '30 days'  -- Get last 30 days
        ORDER BY norad_number, epoch ASC  -- ✅ Sort to maintain time-series order
    """

    df = pd.read_sql(query, engine)  # ✅ Use SQLAlchemy engine
    engine.dispose()  # Close connection when done

    return df