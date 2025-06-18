import os
import duckdb
import pandas as pd
import numpy as np
import uuid
from datetime import datetime, timedelta

# -----------------
# Configurable Parameters
# -----------------

NUM_SENSORS = 30
DAYS = 730  # Generate 2 years of data
INTERVAL_MINUTES = 10

# Fresno ZIPs + coordinates
fresno_zip_locations = {
    '93701': (36.745, -119.785), '93702': (36.752, -119.754),
    '93703': (36.767, -119.750), '93704': (36.790, -119.800),
    '93705': (36.775, -119.823), '93706': (36.707, -119.799),
    '93710': (36.813, -119.771), '93711': (36.840, -119.851),
    '93720': (36.860, -119.760), '93722': (36.800, -119.880),
    '93723': (36.820, -119.960), '93725': (36.680, -119.776),
    '93726': (36.793, -119.760), '93727': (36.750, -119.700),
    '93728': (36.757, -119.815)
}

monthly_temp_ranges = {
    1: (40, 65), 2: (42, 65), 3: (50, 75), 4: (55, 85),
    5: (65, 98), 6: (70, 105), 7: (75, 110), 8: (75, 110),
    9: (70, 100), 10: (60, 90), 11: (50, 70), 12: (40, 60),
}

monthly_pm25_ranges = {
    1: (5, 25), 2: (5, 25), 3: (5, 25), 4: (8, 30),
    5: (10, 35), 6: (15, 40), 7: (20, 120), 8: (25, 120),
    9: (20, 100), 10: (15, 60), 11: (10, 30), 12: (8, 25),
}

# -----------------
# AQI Calculation Function
# -----------------
def calculate_aqi(pm25_array):
    result = []
    for pm in pm25_array:
        bp = [
            (0.0, 12.0, 0, 50), (12.1, 35.4, 51, 100),
            (35.5, 55.4, 101, 150), (55.5, 150.4, 151, 200),
            (150.5, 250.4, 201, 300), (250.5, 350.4, 301, 400),
            (350.5, 500.4, 401, 500)
        ]
        for Clow, Chigh, Ilow, Ihigh in bp:
            if Clow <= pm <= Chigh:
                aqi = ((Ihigh - Ilow) / (Chigh - Clow)) * (pm - Clow) + Ilow
                result.append(round(aqi))
                break
        else:
            result.append(500)  # Cap to 500 if PM2.5 is extremely high
    return np.array(result)

# -----------------
# Generate Sensor Metadata
# -----------------
sensor_metadata = []
zip_codes = list(fresno_zip_locations.keys())

for i in range(NUM_SENSORS):
    zip_code = zip_codes[i % len(zip_codes)]
    base_lat, base_lon = fresno_zip_locations[zip_code]
    lat = round(base_lat + np.random.uniform(-0.005, 0.005), 6)
    lon = round(base_lon + np.random.uniform(-0.005, 0.005), 6)
    sensor_metadata.append({
        "Sensor_ID": f"sensor_{i+1:02d}",
        "Zip_Code": zip_code,
        "Latitude": lat,
        "Longitude": lon
    })

sensor_df = pd.DataFrame(sensor_metadata)

# -----------------
# Generate Timestamp Range
# -----------------
end_time = datetime.now().replace(minute=0, second=0, microsecond=0)
start_time = end_time - timedelta(days=DAYS)
timestamps = pd.date_range(start=start_time, end=end_time, freq=f'{INTERVAL_MINUTES}T')

# -----------------
# Create DuckDB tables
# -----------------
# Dynamically resolve path relative to script location

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

db_path = os.path.join(DATA_DIR, "dummy_air_quality.duckdb")
conn = duckdb.connect(db_path)

conn.execute("DROP TABLE IF EXISTS air_quality")
conn.execute("""
CREATE TABLE air_quality (
    Reading_ID VARCHAR,
    Sensor_ID VARCHAR,
    Longitude DOUBLE,
    Latitude DOUBLE,
    Zip_Code VARCHAR,
    Timestamp TIMESTAMP,
    Temperature DOUBLE,
    PM2_5 DOUBLE,
    AQI INTEGER,
    CIG_APX DOUBLE
)
""")

# -----------------
# Generate & Insert Data Sensor by Sensor
# -----------------
print("🚀 Generating and inserting data...")

for sensor in sensor_metadata:
    all_timestamps = timestamps
    n_rows = len(all_timestamps)

    # Determine seasonal ranges for each timestamp's month
    months = pd.Series(all_timestamps).dt.month.values

    temp_array = np.array([
        np.random.triangular(
            monthly_temp_ranges[m][0],
            (monthly_temp_ranges[m][0] + monthly_temp_ranges[m][1])/2,
            monthly_temp_ranges[m][1]
        ) for m in months
    ])

    pm25_array = np.array([
        np.random.triangular(
            monthly_pm25_ranges[m][0],
            (monthly_pm25_ranges[m][0] + monthly_pm25_ranges[m][1])/2,
            monthly_pm25_ranges[m][1]
        ) for m in months
    ])

    aqi_array = calculate_aqi(pm25_array)
    cig_array = np.round(aqi_array / 22, 2)

    batch_df = pd.DataFrame({
        "Reading_ID": [str(uuid.uuid4()) for _ in range(n_rows)],
        "Sensor_ID": sensor['Sensor_ID'],
        "Longitude": sensor['Longitude'],
        "Latitude": sensor['Latitude'],
        "Zip_Code": sensor['Zip_Code'],
        "Timestamp": all_timestamps,
        "Temperature": np.round(temp_array, 1),
        "PM2_5": np.round(pm25_array, 1),
        "AQI": aqi_array.astype(int),
        "CIG_APX": cig_array
    })

    # Insert directly into DuckDB
    conn.register("batch_df", batch_df)
    conn.execute("INSERT INTO air_quality SELECT * FROM batch_df")

# -----------------
# Create hourly aggregated table
# -----------------
conn.execute("DROP TABLE IF EXISTS air_quality_hourly")
conn.execute("""
CREATE TABLE air_quality_hourly AS
SELECT 
    Sensor_ID,
    Zip_Code,
    Longitude,
    Latitude,
    DATE_TRUNC('hour', Timestamp) AS Hour_Timestamp,
    AVG(Temperature) AS Avg_Temp,
    AVG(PM2_5) AS Avg_PM2_5,
    AVG(AQI) AS Avg_AQI,
    AVG(CIG_APX) AS Avg_CIG_APX
FROM air_quality
GROUP BY Sensor_ID, Zip_Code, Longitude, Latitude, Hour_Timestamp
ORDER BY Hour_Timestamp
""")

conn.close()
print("✅ Done! Production data generated successfully.")
