import json, os

DATA_DIR = os.getenv("DATA_DIR", "./data")
GJ = os.path.join(DATA_DIR, "raw", "zip_shapes.geojson")

def get_zip_geojson():
    with open(GJ) as f:
        gj = json.load(f)
    # keep only the properties we care about
    for feat in gj.get("features", []):
        props = feat.get("properties", {}) or {}
        feat["properties"] = {"zip": props.get("zip"), "name": props.get("name")}
    return gj
