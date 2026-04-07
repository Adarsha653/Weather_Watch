# Databricks notebook source
from pyspark.sql.functions import col, avg, stddev, round as spark_round
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# ── Step 1: build one row per city from historical daily averages ─────────
df_features = (
    spark.table("weather_gold_daily")
    .groupBy("city", "country")
    .agg(
        spark_round(avg("avg_temp_c"),          1).alias("mean_temp_c"),
        spark_round(stddev("avg_temp_c"),        1).alias("temp_variability"),
        spark_round(avg("avg_humidity_pct"),     1).alias("mean_humidity"),
        spark_round(avg("avg_wind_speed_kmh"),   1).alias("mean_wind_kmh"),
        spark_round(avg("total_precipitation_mm"),1).alias("mean_precip_mm")
    )
    .dropna()   # drop any city with nulls in features
)

print(f"Cities going into clustering: {df_features.count()}")
df_features.show(5)

# COMMAND ----------

# ── Step 2: assemble features into a single vector column ────────────────
assembler = VectorAssembler(
    inputCols=["mean_temp_c", "temp_variability", "mean_humidity",
               "mean_wind_kmh", "mean_precip_mm"],
    outputCol="features_raw"
)

# ── Step 3: scale features so temperature doesn't dominate ───────────────
# Without scaling, a temp difference of 10°C would outweigh
# a humidity difference of 30% just because of units
scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=True
)

df_assembled  = assembler.transform(df_features)
scaler_model  = scaler.fit(df_assembled)
df_scaled     = scaler_model.transform(df_assembled)

print("Features assembled and scaled.")

# COMMAND ----------

# ── Step 4: find the best k using silhouette score ───────────────────────
# Silhouette score measures how well cities fit their assigned cluster
# Score ranges from -1 (bad) to 1 (perfect) — higher is better
evaluator = ClusteringEvaluator(featuresCol="features")

results = []
for k in range(3, 9):   # test k=3 through k=8
    kmeans = KMeans(featuresCol="features", k=k, seed=42)
    model  = kmeans.fit(df_scaled)
    preds  = model.transform(df_scaled)
    score  = evaluator.evaluate(preds)
    results.append((k, round(score, 4)))
    print(f"k={k}  silhouette={score:.4f}")

best_k = max(results, key=lambda x: x[1])[0]
print(f"\nBest k: {best_k}")

# COMMAND ----------

import mlflow
import mlflow.spark

mlflow.set_experiment("/weather_clustering_experiment")

# Start the run and keep it open — we'll close it in the next cell
mlflow.start_run(run_name=f"kmeans_k{best_k}")

mlflow.log_param("k", best_k)
mlflow.log_param("features", ["mean_temp_c", "temp_variability",
                               "mean_humidity", "mean_wind_kmh", "mean_precip_mm"])
mlflow.log_param("scaler", "StandardScaler")
mlflow.log_param("cities_count", df_features.count())

for k_val, score in results:
    mlflow.log_metric(f"silhouette_k{k_val}", score)

best_score = max(results, key=lambda x: x[1])[1]
mlflow.log_metric("best_silhouette_score", best_score)

print(f"MLflow run started — k={best_k}, silhouette={best_score:.4f}")

# COMMAND ----------

# ── Step 5: train final model on best k ──────────────────────────────────
kmeans_final = KMeans(featuresCol="features", k=best_k, seed=42)
model_final  = kmeans_final.fit(df_scaled)
df_clustered = model_final.transform(df_scaled)

# ── Step 6: label each cluster with a human-readable climate name ─────────
# Look at cluster centres to understand each group
centers = model_final.clusterCenters()
for i, c in enumerate(centers):
    print(f"Cluster {i}: temp={c[0]:.2f}  variability={c[1]:.2f}  "
          f"humidity={c[2]:.2f}  wind={c[3]:.2f}  precip={c[4]:.2f}")

# COMMAND ----------

# ── Step 7: assign climate labels based on cluster centres ────────────────
# Update this mapping after you see the cluster centre printout above
# These are illustrative — your actual clusters may differ
from pyspark.sql.functions import create_map, lit
from itertools import chain

# Replace with your actual labels after inspecting centres
cluster_labels = {
    0: "Humid & Mild",
    1: "Dry",
    2: "Warm & Humid",
}

mapping_expr = create_map([lit(x) for x in chain(*cluster_labels.items())])

df_final = (
    df_clustered
    .withColumn("climate_cluster", mapping_expr[col("prediction")])
    .select(
        "city", "country",
        "mean_temp_c", "temp_variability",
        "mean_humidity", "mean_wind_kmh", "mean_precip_mm",
        "prediction",
        "climate_cluster"
    )
    .orderBy("climate_cluster", "mean_temp_c")
)

# ── Step 8: write to Gold ─────────────────────────────────────────────────
df_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("weather_gold_city_clusters")

print(f"Clusters written. Total cities: {df_final.count()}")

# Show cluster summary
spark.sql("""
    SELECT climate_cluster, COUNT(*) as city_count,
           ROUND(AVG(mean_temp_c), 1)   AS avg_temp,
           ROUND(AVG(mean_humidity), 1) AS avg_humidity,
           ROUND(AVG(mean_precip_mm),1) AS avg_precip
    FROM weather_gold_city_clusters
    GROUP BY climate_cluster
    ORDER BY avg_temp DESC
""").show()

# COMMAND ----------

import os

# Log cluster sizes
cluster_sizes = df_final.groupBy("climate_cluster").count().collect()
for row in cluster_sizes:
    mlflow.log_metric(
        f"cluster_size_{row['climate_cluster'].replace(' ', '_')}",
        row["count"]
    )

# Save cluster assignments as CSV artifact instead of Spark model
# (Spark ML model saving requires UC volume path on serverless clusters)
cluster_csv_path = "/tmp/cluster_assignments.csv"
df_final.toPandas().to_csv(cluster_csv_path, index=False)
mlflow.log_artifact(cluster_csv_path, "cluster_assignments")

# Log cluster centres as metrics
centers = model_final.clusterCenters()
for i, center in enumerate(centers):
    mlflow.log_metric(f"center_{i}_temp",     round(float(center[0]), 3))
    mlflow.log_metric(f"center_{i}_humidity", round(float(center[2]), 3))
    mlflow.log_metric(f"center_{i}_precip",   round(float(center[4]), 3))

mlflow.end_run()
print("MLflow run complete.")