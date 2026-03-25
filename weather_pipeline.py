import requests
import json
import time
from pyspark.sql.functions import (
    col, to_timestamp, round as spark_round, when, 
    avg, min, max, sum as spark_sum, date_trunc, 
    desc, rank, row_number, current_timestamp
)
from pyspark.sql.window import Window

cities = [
    # North America
    {"city": "New York",       "country": "US",  "lat": 40.7128,  "lon": -74.0060},
    {"city": "Los Angeles",    "country": "US",  "lat": 34.0522,  "lon": -118.2437},
    {"city": "Chicago",        "country": "US",  "lat": 41.8781,  "lon": -87.6298},
    {"city": "Houston",        "country": "US",  "lat": 29.7604,  "lon": -95.3698},
    {"city": "Phoenix",        "country": "US",  "lat": 33.4484,  "lon": -112.0740},
    {"city": "Philadelphia",   "country": "US",  "lat": 39.9526,  "lon": -75.1652},
    {"city": "San Antonio",    "country": "US",  "lat": 29.4241,  "lon": -98.4936},
    {"city": "San Diego",      "country": "US",  "lat": 32.7157,  "lon": -117.1611},
    {"city": "Dallas",         "country": "US",  "lat": 32.7767,  "lon": -96.7970},
    {"city": "San Francisco",  "country": "US",  "lat": 37.7749,  "lon": -122.4194},
    {"city": "Seattle",        "country": "US",  "lat": 47.6062,  "lon": -122.3321},
    {"city": "Denver",         "country": "US",  "lat": 39.7392,  "lon": -104.9903},
    {"city": "Toronto",        "country": "CA",  "lat": 43.6532,  "lon": -79.3832},
    {"city": "Vancouver",      "country": "CA",  "lat": 49.2827,  "lon": -123.1207},
    {"city": "Montreal",       "country": "CA",  "lat": 45.5017,  "lon": -73.5673},
    {"city": "Mexico City",    "country": "MX",  "lat": 19.4326,  "lon": -99.1332},
    {"city": "Guadalajara",    "country": "MX",  "lat": 20.6597,  "lon": -103.3496},
    # South America
    {"city": "São Paulo",      "country": "BR",  "lat": -23.5505, "lon": -46.6333},
    {"city": "Rio de Janeiro", "country": "BR",  "lat": -22.9068, "lon": -43.1729},
    {"city": "Buenos Aires",   "country": "AR",  "lat": -34.6037, "lon": -58.3816},
    {"city": "Lima",           "country": "PE",  "lat": -12.0464, "lon": -77.0428},
    {"city": "Bogotá",         "country": "CO",  "lat": 4.7110,   "lon": -74.0721},
    {"city": "Santiago",       "country": "CL",  "lat": -33.4489, "lon": -70.6693},
    {"city": "Caracas",        "country": "VE",  "lat": 10.4806,  "lon": -66.9036},
    # Europe
    {"city": "London",         "country": "GB",  "lat": 51.5074,  "lon": -0.1278},
    {"city": "Paris",          "country": "FR",  "lat": 48.8566,  "lon": 2.3522},
    {"city": "Berlin",         "country": "DE",  "lat": 52.5200,  "lon": 13.4050},
    {"city": "Madrid",         "country": "ES",  "lat": 40.4168,  "lon": -3.7038},
    {"city": "Rome",           "country": "IT",  "lat": 41.9028,  "lon": 12.4964},
    {"city": "Amsterdam",      "country": "NL",  "lat": 52.3676,  "lon": 4.9041},
    {"city": "Brussels",       "country": "BE",  "lat": 50.8503,  "lon": 4.3517},
    {"city": "Vienna",         "country": "AT",  "lat": 48.2082,  "lon": 16.3738},
    {"city": "Stockholm",      "country": "SE",  "lat": 59.3293,  "lon": 18.0686},
    {"city": "Oslo",           "country": "NO",  "lat": 59.9139,  "lon": 10.7522},
    {"city": "Copenhagen",     "country": "DK",  "lat": 55.6761,  "lon": 12.5683},
    {"city": "Helsinki",       "country": "FI",  "lat": 60.1699,  "lon": 24.9384},
    {"city": "Warsaw",         "country": "PL",  "lat": 52.2297,  "lon": 21.0122},
    {"city": "Prague",         "country": "CZ",  "lat": 50.0755,  "lon": 14.4378},
    {"city": "Budapest",       "country": "HU",  "lat": 47.4979,  "lon": 19.0402},
    {"city": "Bucharest",      "country": "RO",  "lat": 44.4268,  "lon": 26.1025},
    {"city": "Athens",         "country": "GR",  "lat": 37.9838,  "lon": 23.7275},
    {"city": "Lisbon",         "country": "PT",  "lat": 38.7223,  "lon": -9.1393},
    {"city": "Zurich",         "country": "CH",  "lat": 47.3769,  "lon": 8.5417},
    {"city": "Dublin",         "country": "IE",  "lat": 53.3498,  "lon": -6.2603},
    {"city": "Moscow",         "country": "RU",  "lat": 55.7558,  "lon": 37.6173},
    {"city": "Kiev",           "country": "UA",  "lat": 50.4501,  "lon": 30.5234},
    # Africa
    {"city": "Cairo",          "country": "EG",  "lat": 30.0444,  "lon": 31.2357},
    {"city": "Lagos",          "country": "NG",  "lat": 6.5244,   "lon": 3.3792},
    {"city": "Nairobi",        "country": "KE",  "lat": -1.2921,  "lon": 36.8219},
    {"city": "Johannesburg",   "country": "ZA",  "lat": -26.2041, "lon": 28.0473},
    {"city": "Cape Town",      "country": "ZA",  "lat": -33.9249, "lon": 18.4241},
    {"city": "Casablanca",     "country": "MA",  "lat": 33.5731,  "lon": -7.5898},
    {"city": "Accra",          "country": "GH",  "lat": 5.6037,   "lon": -0.1870},
    {"city": "Addis Ababa",    "country": "ET",  "lat": 9.0320,   "lon": 38.7469},
    {"city": "Dar es Salaam",  "country": "TZ",  "lat": -6.7924,  "lon": 39.2083},
    {"city": "Dakar",          "country": "SN",  "lat": 14.7167,  "lon": -17.4677},
    {"city": "Tunis",          "country": "TN",  "lat": 36.8065,  "lon": 10.1815},
    {"city": "Algiers",        "country": "DZ",  "lat": 36.7372,  "lon": 3.0865},
    # Middle East
    {"city": "Dubai",          "country": "AE",  "lat": 25.2048,  "lon": 55.2708},
    {"city": "Riyadh",         "country": "SA",  "lat": 24.7136,  "lon": 46.6753},
    {"city": "Tehran",         "country": "IR",  "lat": 35.6892,  "lon": 51.3890},
    {"city": "Istanbul",       "country": "TR",  "lat": 41.0082,  "lon": 28.9784},
    {"city": "Baghdad",        "country": "IQ",  "lat": 33.3152,  "lon": 44.3661},
    {"city": "Doha",           "country": "QA",  "lat": 25.2854,  "lon": 51.5310},
    {"city": "Kuwait City",    "country": "KW",  "lat": 29.3759,  "lon": 47.9774},
    {"city": "Amman",          "country": "JO",  "lat": 31.9454,  "lon": 35.9284},
    # Asia
    {"city": "Tokyo",          "country": "JP",  "lat": 35.6762,  "lon": 139.6503},
    {"city": "Mumbai",         "country": "IN",  "lat": 19.0760,  "lon": 72.8777},
    {"city": "Delhi",          "country": "IN",  "lat": 28.6139,  "lon": 77.2090},
    {"city": "Bangalore",      "country": "IN",  "lat": 12.9716,  "lon": 77.5946},
    {"city": "Kolkata",        "country": "IN",  "lat": 22.5726,  "lon": 88.3639},
    {"city": "Singapore",      "country": "SG",  "lat": 1.3521,   "lon": 103.8198},
    {"city": "Seoul",          "country": "KR",  "lat": 37.5665,  "lon": 126.9780},
    {"city": "Beijing",        "country": "CN",  "lat": 39.9042,  "lon": 116.4074},
    {"city": "Shanghai",       "country": "CN",  "lat": 31.2304,  "lon": 121.4737},
    {"city": "Shenzhen",       "country": "CN",  "lat": 22.5431,  "lon": 114.0579},
    {"city": "Hong Kong",      "country": "HK",  "lat": 22.3193,  "lon": 114.1694},
    {"city": "Bangkok",        "country": "TH",  "lat": 13.7563,  "lon": 100.5018},
    {"city": "Jakarta",        "country": "ID",  "lat": -6.2088,  "lon": 106.8456},
    {"city": "Kuala Lumpur",   "country": "MY",  "lat": 3.1390,   "lon": 101.6869},
    {"city": "Manila",         "country": "PH",  "lat": 14.5995,  "lon": 120.9842},
    {"city": "Dhaka",          "country": "BD",  "lat": 23.8103,  "lon": 90.4125},
    {"city": "Karachi",        "country": "PK",  "lat": 24.8607,  "lon": 67.0011},
    {"city": "Lahore",         "country": "PK",  "lat": 31.5204,  "lon": 74.3587},
    {"city": "Colombo",        "country": "LK",  "lat": 6.9271,   "lon": 79.8612},
    {"city": "Kathmandu",      "country": "NP",  "lat": 27.7172,  "lon": 85.3240},
    {"city": "Yangon",         "country": "MM",  "lat": 16.8661,  "lon": 96.1951},
    {"city": "Ho Chi Minh",    "country": "VN",  "lat": 10.8231,  "lon": 106.6297},
    {"city": "Hanoi",          "country": "VN",  "lat": 21.0285,  "lon": 105.8542},
    {"city": "Taipei",         "country": "TW",  "lat": 25.0330,  "lon": 121.5654},
    {"city": "Osaka",          "country": "JP",  "lat": 34.6937,  "lon": 135.5023},
    # Oceania
    {"city": "Sydney",         "country": "AU",  "lat": -33.8688, "lon": 151.2093},
    {"city": "Melbourne",      "country": "AU",  "lat": -37.8136, "lon": 144.9631},
    {"city": "Brisbane",       "country": "AU",  "lat": -27.4698, "lon": 153.0251},
    {"city": "Perth",          "country": "AU",  "lat": -31.9505, "lon": 115.8605},
    {"city": "Auckland",       "country": "NZ",  "lat": -36.8509, "lon": 174.7645},
    {"city": "Adelaide",       "country": "AU",  "lat": -34.9285, "lon": 138.6007},
    {"city": "Wellington",     "country": "NZ",  "lat": -41.2866, "lon": 174.7756},
    {"city": "Gold Coast",     "country": "AU",  "lat": -28.0167, "lon": 153.4000},
    {"city": "Canberra",       "country": "AU",  "lat": -35.2809, "lon": 149.1300},
]

def parse_weather_response(response_json, city_name, country):
    hourly = response_json["hourly"]
    records = []
    for i in range(len(hourly["time"])):
        records.append({
            "city":             city_name,
            "country":          country,
            "latitude":         response_json["latitude"],
            "longitude":        response_json["longitude"],
            "timezone":         response_json["timezone"],
            "time":             hourly["time"][i],
            "temperature_c":    hourly["temperature_2m"][i],
            "humidity_pct":     hourly["relative_humidity_2m"][i],
            "wind_speed_kmh":   hourly["wind_speed_10m"][i],
            "precipitation_mm": hourly["precipitation"][i],
            "weather_code":     hourly["weather_code"][i]
        })
    return records

def fetch_city_weather(city_info):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":  city_info["lat"],
        "longitude": city_info["lon"],
        "hourly":    "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,weather_code",
        "timezone":  "auto",
        "forecast_days": 1
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return parse_weather_response(response.json(), city_info["city"], city_info["country"])

all_records = []
failed_cities = []

for city in cities:
    try:
        records = fetch_city_weather(city)
        all_records.extend(records)
    except Exception as e:
        print(f"✗ {city['city']}: FAILED — {e}")
        failed_cities.append(city["city"])
    time.sleep(0.2)

# Retry failed cities once
for city in cities:
    if city["city"] in failed_cities:
        try:
            records = fetch_city_weather(city)
            all_records.extend(records)
            failed_cities.remove(city["city"])
        except Exception:
            pass

print(f"Fetched: {len(all_records)} records | Failed: {failed_cities if failed_cities else 'None'}")

# Write to Bronze
df_bronze = spark.createDataFrame(all_records)
df_bronze = df_bronze.withColumn("ingested_at", current_timestamp())

df_bronze.write.format("delta").mode("append").saveAsTable("weather_bronze")
print("Bronze written.")

def weather_description(code_col):
    return (
        when(code_col == 0,  "Clear sky")
        .when(code_col == 1,  "Mainly clear")
        .when(code_col == 2,  "Partly cloudy")
        .when(code_col == 3,  "Overcast")
        .when(code_col.between(45, 48), "Foggy")
        .when(code_col.between(51, 55), "Drizzle")
        .when(code_col.between(61, 65), "Rain")
        .when(code_col.between(71, 75), "Snow")
        .when(code_col.between(80, 82), "Rain showers")
        .when(code_col.between(95, 99), "Thunderstorm")
        .otherwise("Unknown")
    )

df_silver = (
    spark.table("weather_bronze")
    .withColumn("time", to_timestamp(col("time"), "yyyy-MM-dd'T'HH:mm"))
    .withColumn("feels_like_c", spark_round(
        13.12 + 0.6215 * col("temperature_c")
        - 11.37 * (col("wind_speed_kmh") ** 0.16)
        + 0.3965 * col("temperature_c") * (col("wind_speed_kmh") ** 0.16),
        1
    ))
    .withColumn("weather_description", weather_description(col("weather_code")))
    .drop("latitude", "longitude", "timezone", "weather_code")
    .dropDuplicates(["city", "time"])
    .select(
        "city", "country", "time",
        "temperature_c", "feels_like_c",
        "humidity_pct", "wind_speed_kmh",
        "precipitation_mm", "weather_description",
        "ingested_at"
    )
)

df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("weather_silver")
print("Silver written.")

df_silver = spark.table("weather_silver")

# Gold 1: Daily stats
df_daily = (
    df_silver
    .withColumn("date", date_trunc("day", col("time")))
    .groupBy("city", "country", "date")
    .agg(
        spark_round(avg("temperature_c"), 1).alias("avg_temp_c"),
        spark_round(min("temperature_c"), 1).alias("min_temp_c"),
        spark_round(max("temperature_c"), 1).alias("max_temp_c"),
        spark_round(avg("feels_like_c"), 1).alias("avg_feels_like_c"),
        spark_round(avg("humidity_pct"), 1).alias("avg_humidity_pct"),
        spark_round(avg("wind_speed_kmh"), 1).alias("avg_wind_speed_kmh"),
        spark_round(spark_sum("precipitation_mm"), 1).alias("total_precipitation_mm")
    )
)

df_daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("weather_gold_daily")

# Gold 2: City ranking (global, latest snapshot)
window_latest = Window.partitionBy("city").orderBy(desc("time"))
window_rank = Window.orderBy(desc("temperature_c"))

df_ranking = (
    df_silver
    .withColumn("rn", row_number().over(window_latest))
    .filter(col("rn") == 1)
    .drop("rn")
    .withColumn("temp_rank", rank().over(window_rank))
    .select("temp_rank", "city", "country", "temperature_c", "feels_like_c",
            "humidity_pct", "wind_speed_kmh", "precipitation_mm", "weather_description", "time")
    .orderBy("temp_rank")
)

df_ranking.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("weather_gold_city_ranking")

print("Gold tables written.")
print(f"Daily: {spark.table('weather_gold_daily').count()} rows")
print(f"Ranking: {spark.table('weather_gold_city_ranking').count()} rows")