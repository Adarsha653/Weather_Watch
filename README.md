# Weather Watch

A global weather analytics pipeline built on Databricks, covering 100 cities across 6 continents.

## Architecture
- **Bronze → Silver → Gold** medallion architecture on Databricks
- **100 cities** tracked hourly via Open-Meteo API
- **16 months** of historical data (Dec 2024 → present)

## What it does
- Hourly data pipeline fetching live weather for 100 cities
- Anomaly detection flagging unusual weather events (Z-score)
- City clustering by climate type (K-Means)
- 7-day temperature forecasting (Prophet)
- MLflow experiment tracking for all ML models
- Live Streamlit app pulling data directly from Databricks

## Tech stack
Databricks · PySpark · Delta Lake · Prophet · scikit-learn · MLflow · Streamlit · Plotly

## Setup
1. Clone the repo
2. Create a virtual environment: `python3 -m venv venv && source venv/bin/activate`
3. Install dependencies: `pip install -r requirements.txt`
4. Copy `.env.example` to `.env` and fill in your Databricks credentials
5. Run the app: `streamlit run weather_app.py`

## Notebooks (run in Databricks in this order)
1. `backfill_historical.py` — one-time historical data load
2. `Weather_pipeline.py` — scheduled hourly pipeline
3. `weather_anomaly_detection.py` — Z-score anomaly detection
4. `weather_city_clustering.py` — K-Means climate clustering
5. `weather_forecasting.py` — Prophet 7-day forecasting
