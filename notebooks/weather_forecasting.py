# Databricks notebook source
from prophet import Prophet
from pyspark.sql.functions import col, lit, round as spark_round, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType
import pandas as pd
import warnings
warnings.filterwarnings("ignore")   # Prophet is chatty, suppress its logs

# COMMAND ----------

# Pull all daily city data into Pandas
# Prophet works in Pandas, not Spark — we'll convert back at the end
df_daily = (
    spark.table("weather_gold_daily")
    .select("city", "country", "date", "avg_temp_c")
    .orderBy("city", "date")
    .toPandas()
)

cities_list = df_daily["city"].unique().tolist()
print(f"Cities to forecast: {len(cities_list)}")
print(f"Date range: {df_daily['date'].min()} → {df_daily['date'].max()}")
print(f"Total rows: {len(df_daily):,}")

# COMMAND ----------

import mlflow
import mlflow.prophet

FORECAST_DAYS     = 7
MIN_TRAINING_ROWS = 60
all_forecasts     = []
failed_cities     = []

mlflow.set_experiment("/weather_forecasting_experiment")

with mlflow.start_run(run_name="prophet_all_cities"):

    # Log top-level parameters once
    mlflow.log_param("forecast_days",          FORECAST_DAYS)
    mlflow.log_param("min_training_rows",      MIN_TRAINING_ROWS)
    mlflow.log_param("changepoint_prior_scale",0.05)
    mlflow.log_param("interval_width",         0.80)
    mlflow.log_param("cities_count",           len(cities_list))

    for i, city in enumerate(cities_list):
        city_df = df_daily[df_daily["city"] == city].copy()
        country = city_df["country"].iloc[0]

        if len(city_df) < MIN_TRAINING_ROWS:
            print(f"⚠ Skipping {city} — only {len(city_df)} days")
            continue

        prophet_df = city_df.rename(columns={"date": "ds", "avg_temp_c": "y"})
        prophet_df["ds"] = pd.to_datetime(prophet_df["ds"])

        try:
            # One nested child run per city
            with mlflow.start_run(run_name=f"prophet_{city}", nested=True):

                model = Prophet(
                    yearly_seasonality=True,
                    weekly_seasonality=False,
                    daily_seasonality=False,
                    changepoint_prior_scale=0.05,
                    interval_width=0.80
                )
                model.fit(prophet_df)

                future   = model.make_future_dataframe(periods=FORECAST_DAYS)
                forecast = model.predict(future)

                # Log per-city metrics
                avg_uncertainty = (
                    forecast.tail(FORECAST_DAYS)["yhat_upper"] -
                    forecast.tail(FORECAST_DAYS)["yhat_lower"]
                ).mean()

                mlflow.log_param("city",          city)
                mlflow.log_param("country",       country)
                mlflow.log_param("training_rows", len(city_df))
                mlflow.log_metric("last_actual_temp",
                                  round(float(prophet_df["y"].iloc[-1]), 2))
                mlflow.log_metric("first_forecast_temp",
                                  round(float(forecast.tail(FORECAST_DAYS)["yhat"].iloc[0]), 2))
                mlflow.log_metric("avg_uncertainty_c",
                                  round(float(avg_uncertainty), 2))

                # Save the model
                mlflow.prophet.log_model(model, f"prophet_model_{city}")

                forecast_only = forecast.tail(FORECAST_DAYS)[
                    ["ds", "yhat", "yhat_lower", "yhat_upper"]
                ].copy()
                forecast_only["city"]    = city
                forecast_only["country"] = country
                all_forecasts.append(forecast_only)

        except Exception as e:
            print(f"✗ {city}: {e}")
            failed_cities.append(city)

        if (i + 1) % 10 == 0:
            print(f"✓ {i+1}/{len(cities_list)} cities done")

    # Log summary on the parent run
    mlflow.log_metric("cities_succeeded", len(all_forecasts))
    mlflow.log_metric("cities_failed",    len(failed_cities))

print(f"\nDone. Succeeded: {len(all_forecasts)} | Failed: {failed_cities or 'None'}")

# COMMAND ----------

# Combine all city forecasts into one dataframe
df_all_forecasts = pd.concat(all_forecasts, ignore_index=True)

# Convert back to Spark and write
df_spark = spark.createDataFrame(df_all_forecasts) \
    .withColumnRenamed("ds",         "forecast_date") \
    .withColumnRenamed("yhat",       "predicted_temp_c") \
    .withColumnRenamed("yhat_lower", "predicted_low_c") \
    .withColumnRenamed("yhat_upper", "predicted_high_c") \
    .withColumn("predicted_temp_c",  spark_round(col("predicted_temp_c"), 1)) \
    .withColumn("predicted_low_c",   spark_round(col("predicted_low_c"),  1)) \
    .withColumn("predicted_high_c",  spark_round(col("predicted_high_c"), 1)) \
    .withColumn("generated_at",      current_timestamp())

df_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("weather_gold_forecast")

print(f"Forecast rows written: {df_spark.count()}")

# Preview — pick a city to check
spark.sql("""
    SELECT city, forecast_date, predicted_temp_c, 
           predicted_low_c, predicted_high_c
    FROM weather_gold_forecast
    WHERE city = 'London'
    ORDER BY forecast_date
""").show()