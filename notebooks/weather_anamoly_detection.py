# Databricks notebook source
from pyspark.sql.functions import (
    col, avg, stddev, round as spark_round,
    abs as spark_abs, when, current_timestamp,
    count, lit
)

# ── CONFIG ───────────────────────────────────────────────────────────────
ZSCORE_THRESHOLD = 2.5   # flag anything beyond ±2.5 standard deviations
MIN_HISTORY_DAYS = 30    # need at least 30 days of history per city to compute a reliable baseline
# ─────────────────────────────────────────────────────────────────────────

# Step 1: compute each city's historical baseline from gold_daily
# This gives us mean and stddev per city across all historical data
df_baseline = (
    spark.table("weather_gold_daily")
    .groupBy("city", "country")
    .agg(
        avg("avg_temp_c").alias("hist_mean_temp"),
        stddev("avg_temp_c").alias("hist_std_temp"),
        avg("avg_wind_speed_kmh").alias("hist_mean_wind"),
        stddev("avg_wind_speed_kmh").alias("hist_std_wind"),
        avg("total_precipitation_mm").alias("hist_mean_precip"),
        stddev("total_precipitation_mm").alias("hist_std_precip"),
        count("*").alias("history_days")
    )
    .filter(col("history_days") >= MIN_HISTORY_DAYS)
)

print(f"Cities with enough history: {df_baseline.count()}")

# Step 2: get the latest snapshot per city from gold_daily (today's values)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

window_latest = Window.partitionBy("city").orderBy(desc("date"))

df_today = (
    spark.table("weather_gold_daily")
    .withColumn("rn", row_number().over(window_latest))
    .filter(col("rn") == 1)
    .drop("rn")
    .select("city", "date",
            col("avg_temp_c").alias("today_temp"),
            col("avg_wind_speed_kmh").alias("today_wind"),
            col("total_precipitation_mm").alias("today_precip"))
)

# Step 3: join today's values against the baseline
df_joined = df_today.join(df_baseline, on="city", how="inner")

# Step 4: compute Z-scores
df_zscores = (
    df_joined
    .withColumn("zscore_temp",
        (col("today_temp") - col("hist_mean_temp")) / col("hist_std_temp"))
    .withColumn("zscore_wind",
        (col("today_wind") - col("hist_mean_wind")) / col("hist_std_wind"))
    .withColumn("zscore_precip",
        (col("today_precip") - col("hist_mean_precip")) / col("hist_std_precip"))
)

# Step 5: flag anomalies and describe them in plain English
df_anomalies = (
    df_zscores
    .withColumn("temp_anomaly", spark_abs(col("zscore_temp")) > ZSCORE_THRESHOLD)
    .withColumn("wind_anomaly", spark_abs(col("zscore_wind")) > ZSCORE_THRESHOLD)
    .withColumn("precip_anomaly", spark_abs(col("zscore_precip")) > ZSCORE_THRESHOLD)
    .withColumn("is_anomaly",
        col("temp_anomaly") | col("wind_anomaly") | col("precip_anomaly"))
    .withColumn("anomaly_type",
        when(col("temp_anomaly") & (col("zscore_temp") > 0), "Unusually hot")
        .when(col("temp_anomaly") & (col("zscore_temp") < 0), "Unusually cold")
        .when(col("wind_anomaly") & (col("zscore_wind") > 0), "Unusually windy")
        .when(col("precip_anomaly") & (col("zscore_precip") > 0), "Unusually rainy")
        .otherwise(None))
    .withColumn("severity",
        when(spark_abs(col("zscore_temp")) > 3.5, "Extreme")
        .when(spark_abs(col("zscore_temp")) > 2.5, "High")
        .when(spark_abs(col("zscore_wind")) > 3.5, "Extreme")
        .when(spark_abs(col("zscore_wind")) > 2.5, "High")
        .when(spark_abs(col("zscore_precip")) > 3.5, "Extreme")
        .when(spark_abs(col("zscore_precip")) > 2.5, "High")
        .otherwise(None))
    .filter(col("is_anomaly"))
    .select(
        "city", "country", "date",
        spark_round("today_temp", 1).alias("today_temp_c"),
        spark_round("hist_mean_temp", 1).alias("normal_temp_c"),
        spark_round("zscore_temp", 2).alias("zscore_temp"),
        spark_round("today_wind", 1).alias("today_wind_kmh"),
        spark_round("zscore_wind", 2).alias("zscore_wind"),
        spark_round("today_precip", 1).alias("today_precip_mm"),
        spark_round("zscore_precip", 2).alias("zscore_precip"),
        "anomaly_type", "severity",
        current_timestamp().alias("detected_at")
    )
    .orderBy(desc("zscore_temp"))
)

# Step 6: write to a new Gold table
df_anomalies.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("weather_gold_anomalies")

print(f"Anomalies detected: {df_anomalies.count()}")
df_anomalies.show(20, truncate=False)