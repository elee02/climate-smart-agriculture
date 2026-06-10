# Climate-Smart Agriculture: Large-Scale Crop Yield Prediction Using Satellite Imagery and Historical Weather Data

A fully operational big data pipeline that predicts crop yields by integrating satellite vegetation indices (NDVI) with historical weather patterns across 25 agricultural regions in 5 countries. Built with a polyglot persistence architecture spanning **5 database technologies**, processed with the **Hadoop/Spark ecosystem**, and served through a **Flask web dashboard**.

## Overview

### Problem Statement

How do historical weather patterns and vegetation health influence crop yields across major agricultural regions worldwide, and can we identify areas at risk of declining productivity due to climate shifts?

### Why Big Data?

- **Volume:** Decades of daily satellite imagery (MODIS NDVI rasters at 250m resolution) and billions of weather station records from NOAA's global network produce terabytes of raw data.
- **Variety:** Structured crop statistics (tabular CSV), semi-structured weather station records (CSV/JSON), and unstructured raster imagery (GeoTIFF/HDF) must be analyzed together.
- **Distributed Processing:** Extracting per-region vegetation statistics from raster tiles and joining time-series pixel data with weather observations for every agricultural region requires massive parallelization.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES (Real-World)                            │
│  ┌───────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │ FAOSTAT   │  │ NOAA GSOD    │  │ NASA MODIS   │  │ GADM Boundaries  │   │
│  │ Crop CSV  │  │ Weather CSV  │  │ NDVI GeoTIFF │  │ Shapefiles       │   │
│  └─────┬─────┘  └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘   │
│        │               │                 │                    │             │
│  ┌─────▼─────┐  ┌──────▼───────┐  ┌──────▼───────┐  ┌────────▼─────────┐   │
│  │ PostgreSQL│  │ Flume Agent  │  │ rasterio     │  │ geopandas        │   │
│  │ → HDFS    │  │ (Simulated)  │  │ + rasterstats│  │ + PostGIS        │   │
│  └─────┬─────┘  └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘   │
│        │               │                 │                    │             │
│  ══════╪═══════════════╪═════════════════╪════════════════════╪═════════    │
│        ▼               ▼                 ▼                    ▼             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    HDFS (Distributed Storage)                       │    │
│  │  /data/weather/  /data/satellite/  /data/streaming/  /user/hive/   │    │
│  └─────────────────────────┬───────────────────────────────────────────┘    │
│                            │                                                │
│  ┌─────────────────────────▼───────────────────────────────────────────┐    │
│  │                   PySpark ETL Pipeline                              │    │
│  │  • Zonal Statistics (GeoTIFF → mean NDVI per region)               │    │
│  │  • Weather Cleaning & 16-Day Resampling                            │    │
│  │  • Growing Degree Days (GDD) & Cumulative Rainfall                 │    │
│  │  • Feature Engineering (lag variables, temporal joins)              │    │
│  └───┬──────────┬──────────┬───────────────────────────────────────────┘    │
│      │          │          │                                                │
│      ▼          ▼          ▼                                                │
│  ┌────────┐ ┌────────┐ ┌────────────────┐ ┌───────────┐ ┌───────────┐      │
│  │ HBase  │ │ Hive   │ │ PostgreSQL     │ │ MongoDB   │ │ Spark     │      │
│  │ NoSQL  │ │ ORC    │ │ + PostGIS      │ │ GeoJSON   │ │ Streaming │      │
│  │ NDVI   │ │ Weather│ │ Features+Preds │ │ Dashboard │ │ Real-time │      │
│  │ Series │ │ Indices│ │ Crop Yields    │ │ Maps      │ │ Weather   │      │
│  └────────┘ └────────┘ └───────┬────────┘ └─────┬─────┘ └───────────┘      │
│                                │                │                          │
│  ┌─────────────────────────────▼────────────────▼──────────────────────┐    │
│  │                  Spark MLlib (Random Forest)                        │    │
│  │  • 5-Fold Distributed Cross-Validation                             │    │
│  │  • Hyperparameter Tuning (numTrees × maxDepth grid)                │    │
│  │  • Feature Importance Analysis                                     │    │
│  └─────────────────────────────┬───────────────────────────────────────┘    │
│                                │                                           │
│  ┌─────────────────────────────▼───────────────────────────────────────┐    │
│  │                     Flask Web Dashboard                             │    │
│  │  http://localhost:5000                                              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Real-World Datasets

All data is sourced from openly available repositories:

| Dataset | Format | Source | Coverage |
|---------|--------|--------|----------|
| Crop production statistics (yield, area, production) | Structured CSV | [FAOSTAT](https://www.fao.org/faostat/) | 5 countries, 4 crops, 2015–2019 |
| Daily weather observations (temp, precip) | Semi-structured CSV | [NOAA GSOD](https://www.ncei.noaa.gov/access/search/data-search/global-summary-of-the-day) via [AWS S3](https://noaa-gsod-pds.s3.amazonaws.com/) | 50 stations across 5 countries |
| Satellite vegetation index (NDVI) | Unstructured raster (GeoTIFF/HDF) | [NASA MODIS MOD13Q1](https://lpdaac.usgs.gov/products/mod13q1v061/) | 250m, 16-day composites |
| Administrative boundaries | Shapefile / GeoJSON | [GADM v4.1](https://gadm.org/data.html) | Admin level 1, 5 countries |

### Geographic Scope

25 agricultural regions across 5 major agricultural countries:

| Country | Regions |
|---------|---------|
| **United States** | Iowa, Illinois, Indiana, Nebraska, Kansas |
| **India** | Punjab, Madhya Pradesh, Maharashtra, Uttar Pradesh, Rajasthan |
| **Brazil** | Mato Grosso, Goiás, Paraná, São Paulo, Minas Gerais |
| **China** | Henan, Shandong, Heilongjiang, Jiangsu, Anhui |
| **Kenya** | Uasin Gishu, Trans Nzoia, Nakuru, Nyandarua, Bungoma |

### Crop Focus

Maize, Wheat, Rice, and Soybeans — selected for their global importance and strong representation across all 5 countries.

---

## Database Design (Polyglot Persistence)

Five storage systems were used within one unified architecture, each chosen for its strengths:

| Data Layer | Technology | Purpose |
|------------|------------|---------|
| Structured reference data (county metadata, crop yields, predictions) | **PostgreSQL + PostGIS** | ACID compliance, spatial joins, SQL reporting |
| Semi-structured weather observations (resampled to 16-day intervals) | **Hive external tables on HDFS** (ORC format) | Massive batch aggregation, SQL-on-Hadoop analytics |
| Satellite raster tiles & raw CSVs | **HDFS** (block-replicated binary storage) | Distributed fault-tolerant repository |
| NDVI time-series vegetation indices per region | **HBase** (NoSQL, Thrift API) | Sub-millisecond random reads by composite row key |
| Dashboard data (GeoJSON maps, feature importance, benchmarks) | **MongoDB** | Schema flexibility, fast geospatial queries |

**Integration flow:** Raw satellite tiles land in HDFS → rasterio/rasterstats compute per-region NDVI zonal statistics → results written to HBase (region+date → NDVI) and also aggregated into a Spark DataFrame. Weather data is loaded into Hive as ORC tables, cleaned, and joined with NDVI on region+date. Engineered features flow into Spark MLlib for yield prediction, and output predictions are pushed to PostgreSQL and MongoDB for reporting and dashboards.

---

## Data Processing Pipeline

### Data Download (`download_data.py` & `download_modis_real.py`)

Tools to download real data from all four sources:

```bash
python download_data.py --source all --years 2015 2019
python download_data.py --source fao          # Just FAO crop data
python download_data.py --source noaa         # Just NOAA weather
python download_data.py --source gadm         # Just GADM boundaries

# For MODIS data, use the dedicated earthaccess downloader:
# Using username and password:
python download_modis_real.py --username YOUR_USERNAME --password YOUR_PASSWORD --years 2015 2019

# Alternatively, using an Earthdata Login token:
python download_modis_real.py --token YOUR_TOKEN --years 2015 2019
```

- **FAOSTAT:** Bulk CSV download from FAO's public server, filtered for target countries and crops.
- **NOAA GSOD:** Automated download from AWS S3 public bucket (`noaa-gsod-pds`), no authentication required. Selects stations with best coverage per country.
- **NASA MODIS:** Automated download of MOD13Q1.061 HDF granules via `earthaccess` and automatic conversion to GeoTIFF (requires free NASA Earthdata account).
- **GADM:** Downloads admin-level-1 shapefiles for all 5 countries.

### Data Ingestion (`data_ingest.py`)

Loads downloaded data into the polyglot database layer:
- GADM shapefiles → PostGIS (real geometries) + MongoDB (GeoJSON)
- FAOSTAT CSV → PostgreSQL `crop_yields` table (fully matches Soybean statistics by mapping "soy beans" variants, distributed to 25 regions with regional variance)
- NOAA weather → consolidated CSV mapped to nearest agricultural region
- MODIS GeoTIFF → hybrid zonal NDVI statistics via rasterio + rasterstats. It processes available GeoTIFF tiles, automatically falls back to phenology-based NDVI records for missing regions/years (2015-2019), and caches calculated zonal stats in `data/processed_tiles_cache.json` to optimize subsequent runs.

### PostgreSQL → HDFS Export (`sqoop_ingest.py`)

Exports relational data from PostgreSQL into the HDFS warehouse via WebHDFS, following the Apache Sqoop import pattern:

```bash
python sqoop_ingest.py import \
  --connect jdbc:postgresql://postgres:5432/crop_yield_db \
  --table crop_yields --target-dir /user/hive/warehouse/crop_yields
```

### PySpark ETL Pipeline (`pipeline.py`)

1. **Satellite Processing:** Reads NDVI pixel data from HDFS. If MODIS GeoTIFF files are present, computes zonal statistics using rasterio and rasterstats against GADM polygons. Aggregates to mean NDVI per region per 16-day composite.
2. **HBase Write:** NDVI time-series stored in HBase with composite row keys (`{county_id}_{date}`), enabling sub-millisecond prefix scans per region.
3. **Weather Processing:** Cleans and imputes NOAA observations (bounds temperature to -40°C–55°C, removes negative precipitation). Resamples daily data to 16-day intervals matching MODIS revisit frequency. Computes Growing Degree Days (GDD, base 10°C) and cumulative rainfall.
4. **Hive ORC:** Writes weather indices to Hive warehouse in ORC columnar format for efficient batch analytics.
5. **Feature Engineering:** Joins weather and NDVI via Spark SQL. Creates lag features (NDVI and precipitation at t-1, t-2) using window functions. Outputs to PostgreSQL via JDBC.

### Flume Ingestion Agent (`flume_agent.py`)

Implements the Apache Flume Source-Channel-Sink architecture pattern to ingest live weather data:
- **Spooling Directory Source:** Monitors a directory for new weather CSV files
- **Memory Channel:** In-memory buffer with configurable capacity (10,000 records)
- **HDFS Sink:** Batches records and writes to HDFS via WebHDFS in configurable intervals

The producer fetches **real-time weather observations** from the [Open-Meteo API](https://open-meteo.com/) for all 25 agricultural regions, writing CSV files that the agent picks up and streams into HDFS.

### Spark Structured Streaming (`spark_streaming.py`)

Monitors the HDFS streaming directory for new weather observations (produced by Flume) and processes them in near-real-time:
- Reads new CSV files as a streaming DataFrame with explicit schema
- Cleans and validates incoming records
- Computes running aggregates per county per date
- Writes updated summaries to HBase via `foreachBatch` sink
- Uses HDFS-based checkpointing for fault tolerance

### Persistent Background Streaming Pipeline
After the initial ETL completes, `run_all.sh` spins up both the **Flume Agent** and **Spark Structured Streaming** to run continuously in the background inside the container:
- The streaming pipeline continues to fetch live weather data and update HBase.
- Log outputs are routed to `data/flume_agent.log` and `data/spark_streaming.log` on the host for real-time monitoring.
- The processes run indefinitely until all containers are stopped via `docker-compose down`.

---

## Machine Learning (`ml_model.py`)

### Random Forest Regression

A Random Forest model was trained using Spark MLlib to predict crop yield (mt/ha) from satellite and weather features:

**Input features:**
- `mean_ndvi` — Annual average NDVI
- `max_ndvi` — Peak NDVI during growing season
- `annual_precip` — Total annual precipitation (mm)
- `mean_temp` — Average annual temperature (°C)
- `annual_gdd` — Cumulative Growing Degree Days
- `area_ha` — Harvested area (hectares)

### Distributed Cross-Validation

5-fold cross-validation was performed with distributed hyperparameter tuning over a 3×3 parameter grid:

| Parameter | Values |
|-----------|--------|
| `numTrees` | 20, 35, 50 |
| `maxDepth` | 4, 6, 8 |

This produces 9 combinations × 5 folds = **45 models trained in parallel** across the Spark cluster. The best model and all fold metrics are logged to MongoDB.

---

## Performance Benchmarks (`benchmark.py`)

### SQL vs NoSQL Comparison

Compared query latency across three storage systems for different access patterns:

| System | Query Type | Expected Latency |
|--------|-----------|------------------|
| **HBase** | Single-row `Get` by composite key | < 1 ms |
| **PostgreSQL** | Indexed point query | ~1–5 ms |
| **Hive / Spark SQL** | Full-table aggregation scan | ~500–5000 ms |

HBase excels at point lookups, while Hive/Spark SQL is optimized for batch aggregation over billions of rows.

### Scaling Analysis

Measured aggregation execution time over increasing data fractions (10%, 25%, 50%, 75%, 100%) to demonstrate sub-linear scaling behavior of distributed Spark processing.

### ORC vs CSV Format Comparison

Compared identical aggregation queries on:
- Hive ORC tables (columnar, compressed, predicate pushdown)
- Raw CSV files on HDFS (row-oriented, uncompressed)

ORC consistently outperforms CSV for analytical workloads due to columnar storage and statistics-based predicate pushdown.

---

## Project Structure

```
climate-smart-agriculture/
├── README.md                 # This file
├── LICENSE                   # MIT License
├── Dockerfile                # Python 3.10 + Java 21 + GDAL + PySpark
├── docker-compose.yml        # 7 services: Postgres, MongoDB, HBase, HDFS, Spark, App
├── hadoop.env                # Hadoop configuration environment variables
├── spark-defaults.conf       # Spark Java module access configuration
├── requirements.txt          # Python dependencies (17 packages)
│
├── download_data.py          # [Phase 1] Real data download from FAO, NOAA, GADM
├── download_modis_real.py    # [Phase 1] Automated NASA MODIS Real Data Downloader via earthaccess
├── data_ingest.py            # [Phase 2] Load data into PostgreSQL, MongoDB, prepare CSVs
├── sqoop_ingest.py           # [Phase 3] PostgreSQL → HDFS export via WebHDFS
├── pipeline.py               # [Phase 4] PySpark ETL: zonal stats, cleaning, features
├── flume_agent.py            # [Phase 5] Flume-style ingestion agent (live Open-Meteo API data)
├── spark_streaming.py        # [Phase 6] Spark Structured Streaming job
├── ml_model.py               # [Phase 7] Spark MLlib Random Forest + cross-validation
├── benchmark.py              # [Phase 8] Performance benchmarks (HBase vs SQL vs Hive)
├── run_all.sh                # End-to-end pipeline orchestration script
│
├── app/
│   ├── main.py               # Flask web dashboard (8 API endpoints)
│   └── templates/
│       └── index.html        # Dashboard frontend (maps, charts, tables)
│
└── data/
    ├── raw/                  # Downloaded real-world data
    │   ├── fao/              # FAOSTAT crop production CSV
    │   ├── noaa/             # NOAA GSOD station data
    │   ├── modis/            # MODIS NDVI GeoTIFF tiles
    │   └── gadm/             # GADM admin boundary shapefiles
    ├── processed/            # Pipeline-ready processed CSVs
    ├── crop_yields.csv       # Processed crop yield data
    ├── weather_observations.csv  # Processed weather data
    └── satellite_ndvi_pixels.csv # Processed NDVI zonal stats
```

---

## Docker Infrastructure

| Service | Image | Ports | Purpose |
|---------|-------|-------|---------|
| `postgres` | postgis/postgis:15-3.3-alpine | 5432 | RDBMS + spatial data |
| `mongodb` | mongo:6.0 | 27017 | Document store + GeoJSON |
| `hbase` | harisekhon/hbase:1.4 | 16010, 9090, 2181 | NoSQL + Thrift API |
| `namenode` | bde2020/hadoop-namenode | 9870, 9000 | HDFS NameNode |
| `datanode` | bde2020/hadoop-datanode | 9864 | HDFS DataNode |
| `spark` | apache/spark:3.5.0 | 8080, 7077 | Spark Master |
| `app` | custom (Dockerfile) | 5000 | Pipeline runner + Flask dashboard |

---

## How to Run

### Prerequisites

- **Docker** and **Docker Compose** installed
- **Python 3.10+** (for data download script, runs on host)
- **NASA Earthdata account** (free, for MODIS GeoTIFF download — optional)
- **Internet connection** (for data download)

### Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/elee02/climate-smart-agriculture.git
cd climate-smart-agriculture

# 2. Download real-world data (runs on host)
pip install requests pandas numpy geopandas shapely
python download_data.py --source all --years 2015 2019

# 3. Download MODIS data via earthaccess (requires NASA Earthdata account)
# Using username and password:
python download_modis_real.py --username YOUR_USERNAME --password YOUR_PASSWORD --years 2015 2019
# Or using Earthdata Login token:
python download_modis_real.py --token YOUR_TOKEN --years 2015 2019

# 4. Run the full pipeline (Docker)
chmod +x run_all.sh
bash run_all.sh
```

### Pipeline Steps (run_all.sh)

| Step | Description | Tool |
|------|-------------|------|
| 0 | Download real-world data | `download_data.py`, `download_modis_real.py` |
| 1 | Start Docker infrastructure | `docker-compose up -d` |
| 2 | Wait for services to boot | 40s health check |
| 3 | Ingest data into databases + HDFS | `data_ingest.py` (with zonal stats cache & hybrid fallback) |
| 4 | Export crop yields from PostgreSQL → HDFS | `sqoop_ingest.py` (with Soybean mappings) |
| 5 | PySpark ETL pipeline | `pipeline.py` |
| 6 | Flume ingestion (live weather from Open-Meteo API) | `flume_agent.py` |
| 7 | Spark Structured Streaming demo | `spark_streaming.py` |
| 8 | ML model training + cross-validation | `ml_model.py` |
| 9 | Performance benchmarks | `benchmark.py` |
| 10 | Start persistent background streaming pipeline | `flume_agent.py` & `spark_streaming.py` |

### Web Interfaces

After pipeline completion:

| Interface | URL |
|-----------|-----|
| **Dashboard** | http://localhost:5000 |
| **HDFS Web UI** | http://localhost:9870 |
| **HBase Web UI** | http://localhost:16010 |
| **Spark Web UI** | http://localhost:8080 |

---

## Challenges & Solutions

| Challenge | Solution |
|-----------|----------|
| Different spatial resolutions (250m MODIS pixels → irregular admin polygons) | Zonal statistics via rasterio + rasterstats with CRS reprojection |
| FAO data is country-level, pipeline needs region-level | Distributed to regions with ±15% variance based on agricultural baselines |
| NOAA station sparsity in some countries (e.g., Kenya) | Nearest-station mapping using spatial distance to region centroids |
| Data skew across countries | Custom region-balanced partitioning in Spark |
| MODIS NoData values and scale factors | Handled -3000 fill values, applied ÷10000 scaling in GeoTIFF processing |
| Real-time ingestion without full Flume/Kafka | Implemented Flume Source-Channel-Sink pattern with live Open-Meteo API data and HDFS persistence |