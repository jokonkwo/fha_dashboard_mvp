import os, pandas as pd

DATA_DIR = os.getenv("DATA_DIR", "./data")
CSV = os.path.join(DATA_DIR, "processed", "aqi_timeseries.csv")

def get_summary(start, end, zip=None):
    df = pd.read_csv(CSV, parse_dates=["timestamp"])
    start = pd.to_datetime(start); end = pd.to_datetime(end)
    m = (df["timestamp"] >= start) & (df["timestamp"] <= end)
    if zip:
        m &= (df["zip"] == zip)
    df = df.loc[m, ["timestamp", "zip", "sensor_id", "pm25", "aqi"]].sort_values("timestamp")

    stats = {
        "mean": float(df["aqi"].mean()) if not df.empty else None,
        "p95": float(df["aqi"].quantile(0.95)) if not df.empty else None,
        "max": float(df["aqi"].max()) if not df.empty else None,
    }
    return {"timeseries": df.to_dict(orient="records"), "stats": stats, "meta": {"source": "synthetic"}}
