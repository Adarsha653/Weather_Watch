# Weather Watch

A global weather data pipeline built with PySpark that fetches hourly weather data for 100+ cities worldwide using the Open-Meteo API, designed to run as a scheduled job on Databricks.

## Architecture

```mermaid
flowchart LR
    A[🌐 Open-Meteo API] -->|Hourly fetch\n100+ cities| B[PySpark Ingestion]

    subgraph Databricks Medallion Pipeline
        B --> C[🥉 Bronze Layer\nweather_bronze\nRaw append-only records]
        C --> D[🥈 Silver Layer\nweather_silver\nCleaned · Deduplicated\nFeels-like temp · Weather labels]
        D --> E1[🥇 Gold — Daily Stats\nweather_gold_daily\nAvg · Min · Max per city/day]
        D --> E2[🥇 Gold — City Ranking\nweather_gold_city_ranking\nLatest snapshot · Global rankings]
    end

    E1 --> F[📊 Databricks Dashboard\nKPI tiles · World map]
    E2 --> F

    G[⏰ Databricks Job\n1-hour schedule] -.->|triggers| B
```

## Overview

This pipeline collects real-time weather data every hour across major cities in North America, South America, Europe, Africa, the Middle East, Asia, and Oceania.

## Data Collected

| Field | Description |
|---|---|
| `temperature_c` | Temperature in Celsius |
| `humidity_pct` | Relative humidity (%) |
| `wind_speed_kmh` | Wind speed (km/h) |
| `precipitation_mm` | Precipitation (mm) |
| `weather_code` | WMO weather condition code |

## Tech Stack

- **PySpark** — distributed data processing
- **Open-Meteo API** — free, no-key-required weather API
- **Databricks** — scheduled job execution (hourly refresh)

## Setup

### Local
```bash
pip install -r requirements.txt
python weather_pipeline.py
```

### Databricks
1. Import this repo via **Databricks Repos** (Repos → Add Repo → paste GitHub URL)
2. Create a new **Job** pointing to `weather_pipeline.py`
3. Set the schedule to **every 1 hour**

## Dashboard

The pipeline feeds a live Databricks dashboard with global weather KPIs and an interactive world map.

![Weather Dashboard](assets/dashboard_1.png)
![Weather Dashboard Map](assets/dashboard_2.png)

> Dashboard exported as `dashboards/Weather_Dashboard.lvdash.json` — import it directly into Databricks.

## Cities Covered

100 cities across 7 regions:
- North America, South America, Europe, Africa, Middle East, Asia, Oceania
