## Project: **Climate-Smart Agriculture** – Large-Scale Crop Yield Prediction Using Satellite Imagery and Historical Weather Data

### 1. Problem Definition & Big Data Relevance
**Problem:** How do historical weather patterns and vegetation health (from satellite imagery) influence crop yields across thousands of counties worldwide, and can we identify regions at risk of food insecurity due to climate shifts?  
**Why Big Data:**
- **Volume:** Decades of daily satellite imagery (e.g., MODIS NDVI rasters) and billions of weather records from global stations easily exceed terabytes.
- **Variety:** Structured crop statistics (tabular), semi-structured weather station JSON/CSV, and unstructured raster imagery (GeoTIFF/HDF) must be analyzed together.
- **Distributed processing need:** Extracting and joining time-series pixel data with weather time series for every agricultural region requires massive parallelization; a single machine cannot complete the task in reasonable time.

### 2. Real-World Datasets
All data is openly available:

| Dataset | Size/Type | Source |
|---------|-----------|--------|
| Crop production statistics (yield, area) | Structured (CSV) | FAO STAT (global, per country/region, yearly) |
| Daily weather observations | Semi-structured (CSV/JSON) | NOAA GSOD (tens of thousands of stations, 1929–present) |
| Satellite vegetation index (NDVI/EVI) | Unstructured raster (HDF/GeoTIFF) | NASA MODIS (MOD13Q1, 250m, 16-day composites, global coverage) |
| Administrative boundaries | Structured (Shapefile/GeoJSON) | GADM (world regions, stored in PostGIS) |

Total raw volume can easily reach **500 GB–1 TB**, justifying a distributed storage and processing approach.

### 3. Database Design (Polyglot Persistence)
You will use **four different storage systems** within one unified architecture, showcasing integration and comparison.

| Data Layer | Technology | Purpose |
|------------|------------|---------|
| Structured reference data (county/crop metadata, final aggregated results) | **RDBMS (PostgreSQL + PostGIS)** | ACID compliance, spatial joins, SQL dashboarding |
| Semi-structured weather observations (billions of rows) | **Hive external tables on HDFS** (ORC format) | Massive batch aggregation, SQL-on-Hadoop analytics |
| Satellite raster tiles (raw imagery) | **HDFS** (binary storage, block replication) | Distributed fault-tolerant repository for image data |
| Extracted time-series vegetation indices per region | **NoSQL (HBase or Cassandra)** | Fast random reads of long pixel time series by region key |
| Quick-look aggregated maps for visualization | **MongoDB** (GeoJSON documents) | Schema flexibility, fast geospatial queries for a web dashboard |

**Integration:** A typical pipeline: raw satellite tiles land in HDFS → Spark job extracts per-county average NDVI over time → results written to HBase (region, date → value) and also aggregated into Hive tables. Weather data is ingested into Hive, cleaned, and joined with NDVI on region+date. Final joined features are pulled into Spark MLlib for yield prediction, and output tables are pushed back to PostgreSQL for reporting.

### 4. Data Processing & Analysis (Hadoop Ecosystem)
Build a complete, end-to-end pipeline using multiple ecosystem tools:

- **Ingestion:** Sqoop to import FAO crop CSV into Hive; custom Python scripts to download MODIS tiles directly into HDFS; Flume (simulated) to stream weather updates into HDFS.
- **Cleaning & Transformation:** Spark (PySpark) jobs that:
  - Parse HDF raster tiles and compute zonal statistics (mean NDVI per county) using vectorized UDFs.
  - Clean and impute missing weather values over a distributed DataFrame.
  - Resample time series to a common 16-day interval to match satellite revisit time.
- **Querying & Aggregation:** Hive queries to compute seasonal climate indices (e.g., growing degree days, cumulative rainfall) for each region and crop year. Demonstrate complex joins between the weather fact table and the region dimension table stored in Hive.
- **Advanced Analytics:** Spark MLlib to train a random forest regression model predicting crop yield from NDVI + weather features. Use **distributed cross-validation** and feature importance analysis.
- **Streaming (optional advanced layer):** If live MODIS data were streamed, use Spark Streaming to update HBase dashboards in near real time for current season monitoring.

### 5. Results & Discussion
**Insights:**
- Identify which climate variables (e.g., drought stress during grain filling) most impact maize yield in Sub-Saharan Africa.
- Visualize on a map how predicted yield anomalies correlate with observed NDVI deviations.

**Scalability & Performance Observations:**
- Benchmark the same aggregation written in Hive (MapReduce engine) vs. Spark SQL on ORC data, showing Spark’s in-memory advantage for iterative workloads.
- Demonstrate linear scaling: process 1 year vs. 10 years of satellite data; measure job duration and show it scales sub-linearly with added nodes.
- **SQL vs NoSQL comparison:** Compare query latency when retrieving a single county’s 20-year NDVI time series from Hive (full table scan unless partitioned) vs. HBase (single-row `Get` by composite key). HBase will be orders of magnitude faster for point lookups, while Hive excels at aggregating over billions of rows across all regions.

**Challenges:**  
- Handling different spatial resolutions (250m pixels aggregated to irregular county polygons).  
- Data skew in weather stations (many stations in the US, few in Africa) – solve with custom partitioning.  
- Balancing task parallelism when processing large raster files.