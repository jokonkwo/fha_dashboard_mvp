from fastapi import APIRouter, Query
from datetime import datetime
from typing import Optional
from app.services import aqi_summary, geojson, sensor_counts

router = APIRouter()

@router.get("/aqi-summary")
def get_aqi_summary(
    start: datetime = Query(..., description="ISO time"),
    end: datetime = Query(..., description="ISO time"),
    zip: Optional[str] = Query(None)
):
    return aqi_summary.get_summary(start, end, zip)

@router.get("/geojson")
def get_geojson():
    return geojson.get_zip_geojson()

@router.get("/sensor-counts")
def get_sensor_counts(zip: str = Query(...)):
    return sensor_counts.get_counts(zip)
