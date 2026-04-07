from databricks import sql
import pandas as pd
import os
from dotenv import load_dotenv

# ── LOAD ENVIRONMENT VARIABLES ──────────────────────────────────────────────────
load_dotenv()

SERVER_HOSTNAME = os.getenv("SERVER_HOSTNAME")
HTTP_PATH       = os.getenv("HTTP_PATH")
ACCESS_TOKEN    = os.getenv("ACCESS_TOKEN")
CATALOG         = os.getenv("CATALOG")
SCHEMA          = os.getenv("SCHEMA")

def run_query(sql_query):
    with sql.connect(
        server_hostname = SERVER_HOSTNAME,
        http_path       = HTTP_PATH,
        access_token    = ACCESS_TOKEN
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(result, columns=columns)

def get_city_ranking():
    return run_query(f"""
        SELECT temp_rank, city, country, temperature_c,
               feels_like_c, humidity_pct, wind_speed_kmh,
               weather_description
        FROM {CATALOG}.{SCHEMA}.weather_gold_city_ranking
        ORDER BY temp_rank
    """)

def get_anomalies():
    return run_query(f"""
        SELECT city, country, anomaly_type, severity,
               today_temp_c, normal_temp_c, zscore_temp
        FROM {CATALOG}.{SCHEMA}.weather_gold_anomalies
        ORDER BY ABS(zscore_temp) DESC
    """)

def get_forecast(city):
    return run_query(f"""
        SELECT forecast_date, predicted_temp_c,
               predicted_low_c, predicted_high_c
        FROM {CATALOG}.{SCHEMA}.weather_gold_forecast
        WHERE city = '{city}'
        ORDER BY forecast_date
    """)

def get_clusters():
    return run_query(f"""
        SELECT city, country, climate_cluster,
               mean_temp_c, mean_humidity
        FROM {CATALOG}.{SCHEMA}.weather_gold_city_clusters
        ORDER BY climate_cluster, mean_temp_c DESC
    """)

# Quick test
if __name__ == "__main__":
    df = get_city_ranking()
    print(f"Connected! Top 5 cities:")
    print(df.head())
