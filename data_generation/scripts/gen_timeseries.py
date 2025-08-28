import csv, math, random, pathlib
from datetime import datetime, timedelta
ROOT = pathlib.Path(__file__).resolve().parents[2]
SENSORS = ROOT / "data" / "raw" / "sensors_seed.csv"
OUT = ROOT / "data" / "processed" / "aqi_timeseries.csv"
random.seed(42)
def pm25_to_aqi(pm25):
    brks=[(0.0,12.0,0,50),(12.1,35.4,51,100),(35.5,55.4,101,150),(55.5,150.4,151,200),(150.5,250.4,201,300),(250.5,350.4,301,400),(350.5,500.4,401,500)]
    for cL,cH,aL,aH in brks:
        if pm25<=cH: return round((aH-aL)/(cH-cL)*(pm25-cL)+aL)
    return 500
def load_sensors():
    with open(SENSORS) as f: return list(csv.DictReader(f))
def main():
    sensors=load_sensors(); OUT.parent.mkdir(parents=True, exist_ok=True)
    end=datetime.utcnow().replace(minute=0, second=0, microsecond=0); start=end - timedelta(days=7)
    ts=start; rows=[]
    while ts<=end:
        hour=ts.hour; base=8 + 6*math.sin((hour/24)*2*math.pi) + 0.5*random.random()
        for s in sensors:
            zf={"93727":1.2,"93720":0.8,"93706":1.1}.get(s["zip"],1.0)
            spike = random.uniform(20,60) if random.random()<0.02 else 0
            pm25=max(1.0, base*zf + random.uniform(-2,3) + spike)
            aqi=pm25_to_aqi(pm25)
            rows.append({"timestamp": ts.isoformat()+"Z","zip": s["zip"],"sensor_id": s["sensor_id"],
                         "lat": s["lat"],"lon": s["lon"],"pm25": f"{pm25:.2f}","aqi": aqi,
                         "quality_flag":"ok","source":"synthetic"})
        ts += timedelta(hours=1)
    with open(OUT,"w",newline="") as f:
        w=csv.DictWriter(f, fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)
    print(f"Wrote {OUT} with {len(rows)} rows")
if __name__ == "__main__": main()