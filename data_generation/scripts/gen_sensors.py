import csv, random, pathlib
ROOT = pathlib.Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "raw" / "sensors_seed.csv"
random.seed(42)
ZIP_CENTERS = {"93727": (36.73, -119.68), "93720": (36.87, -119.79), "93706": (36.69, -119.82)}
def jitter(v, spread=0.02): return v + random.uniform(-spread, spread)
def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    rows, sid = [], 1
    for zip_, (lat, lon) in ZIP_CENTERS.items():
        for _ in range(6):  # 6 sensors per zip
            rows.append({"sensor_id": f"S-{sid:03d}","zip": zip_,"lat": f"{jitter(lat):.5f}","lon": f"{jitter(lon):.5f}",
                         "install_date":"2025-05-01","model":"FAKE-PA"}); sid += 1
    with open(OUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)
    print(f"Wrote {OUT} ({len(rows)} sensors)")
if __name__ == "__main__": main()
