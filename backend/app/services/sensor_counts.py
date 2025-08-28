import os, pandas as pd

DATA_DIR = os.getenv("DATA_DIR", "./data")
SENS = os.path.join(DATA_DIR, "raw", "sensors_seed.csv")

def get_counts(zip):
    df = pd.read_csv(SENS)
    n = int((df["zip"] == zip).sum())
    return {"zip": zip, "sensors": n}
