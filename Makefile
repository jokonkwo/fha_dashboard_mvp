# Generate synthetic data (sensors + AQI timeseries)
seed:
	python3 data_generation/scripts/gen_sensors.py
	python3 data_generation/scripts/gen_timeseries.py

# Run the FastAPI backend
api:
	PYTHONPATH=backend DATA_DIR=./data uvicorn app.main:app --reload --port 8000 --app-dir backend
