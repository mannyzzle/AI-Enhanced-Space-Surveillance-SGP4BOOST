from database import get_db_connection
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sgp4.api import Satrec
from datetime import datetime

# ✅ Fetch TLE Data & Extract B* from TLE Lines
def fetch_tle_bstar(norad_number=56851):
    conn = get_db_connection()

    query = f"""
        SELECT epoch, tle_line1, tle_line2
        FROM satellite_tle_history
        WHERE norad_number = {norad_number}
        ORDER BY epoch;
    """
    tle_df = pd.read_sql(query, conn)
    conn.close()

    # ✅ Convert `epoch` to datetime
    tle_df["epoch"] = pd.to_datetime(tle_df["epoch"])

    # ✅ Extract B* from TLE using SGP4 library
    bstar_values = []
    for _, row in tle_df.iterrows():
        satrec = Satrec.twoline2rv(row["tle_line1"], row["tle_line2"])
        bstar_values.append(satrec.bstar)  # Extract B* value

    tle_df["bstar"] = bstar_values  # Add B* to dataframe
    return tle_df


# ✅ Fetch Space Weather Data (Matching TLE Epochs)
def fetch_space_weather():
    conn = get_db_connection()

    query = """
        SELECT epoch, imf_gsm_bz, geo_dst, sw_speed
        FROM unified_space_weather
        ORDER BY epoch;
    """
    weather_df = pd.read_sql(query, conn)
    conn.close()

    # ✅ Convert `epoch` to datetime
    weather_df["epoch"] = pd.to_datetime(weather_df["epoch"])
    return weather_df


# ✅ Match TLE Epochs to Nearest Space Weather Epochs
def match_epochs(tle_df, weather_df):
    tle_df["closest_weather_epoch"] = tle_df["epoch"].apply(
        lambda x: weather_df.iloc[(weather_df["epoch"] - x).abs().idxmin()]["epoch"]
    )
    
    # ✅ Merge TLE (B*) with Weather Data
    matched_data = tle_df.merge(weather_df, left_on="closest_weather_epoch", right_on="epoch", suffixes=('_tle', '_weather'))
    return matched_data


# ✅ Plot B* vs Space Weather Metrics
def plot_bstar_vs_weather(matched_data):
    fig, axes = plt.subplots(3, 1, figsize=(10, 12), sharex=True)

    # ✅ Plot B* vs IMF Bz
    axes[0].plot(matched_data["epoch_tle"], matched_data["bstar"], label="B* (drag coefficient)", color="black")
    ax2 = axes[0].twinx()
    ax2.plot(matched_data["epoch_tle"], matched_data["imf_gsm_bz"], label="IMF Bz (nT)", color="red", linestyle="dashed")

    axes[0].set_ylabel("B* (1/Earth Radii)")
    ax2.set_ylabel("IMF Bz (nT)", color="red")
    axes[0].legend(loc="upper left")
    ax2.legend(loc="upper right")
    axes[0].set_title("B* vs IMF Bz")

    # ✅ Plot B* vs Dst Index
    axes[1].plot(matched_data["epoch_tle"], matched_data["bstar"], label="B* (drag coefficient)", color="black")
    ax3 = axes[1].twinx()
    ax3.plot(matched_data["epoch_tle"], matched_data["geo_dst"], label="Dst Index (nT)", color="blue")

    axes[1].set_ylabel("B* (1/Earth Radii)")
    ax3.set_ylabel("Dst Index (nT)", color="blue")
    axes[1].legend(loc="upper left")
    ax3.legend(loc="upper right")
    axes[1].set_title("B* vs Dst Index")

    # ✅ Plot B* vs Solar Wind Speed
    axes[2].plot(matched_data["epoch_tle"], matched_data["bstar"], label="B* (drag coefficient)", color="black")
    ax4 = axes[2].twinx()
    ax4.plot(matched_data["epoch_tle"], matched_data["sw_speed"], label="Solar Wind Speed (km/s)", color="green", linestyle="dashed")

    axes[2].set_ylabel("B* (1/Earth Radii)")
    ax4.set_ylabel("Solar Wind Speed (km/s)", color="green")
    axes[2].legend(loc="upper left")
    ax4.legend(loc="upper right")
    axes[2].set_title("B* vs Solar Wind Speed")

    plt.xlabel("Time")
    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.show()


# ✅ Main Execution
if __name__ == "__main__":
    print("🔄 Fetching TLE data & extracting B* values...")
    tle_data = fetch_tle_bstar()

    print("🔄 Fetching Space Weather data...")
    weather_data = fetch_space_weather()

    print("🔄 Matching TLE epochs with closest weather epochs...")
    matched_data = match_epochs(tle_data, weather_data)

    print("📈 Plotting B* vs Space Weather Metrics...")
    plot_bstar_vs_weather(matched_data)

    print("✅ Done!")
