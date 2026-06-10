#!/usr/bin/env python3
"""
Climate-Smart Agriculture — PySpark ETL Pipeline

End-to-end data processing pipeline:
  1. Read satellite NDVI data (from GeoTIFF zonal stats CSV) from HDFS
     and compute per-region average NDVI. Write time-series to HBase.
  2. Read NOAA weather observations from HDFS, clean, impute, and
     resample to 16-day intervals. Compute seasonal climate indices
     (GDD, cumulative rainfall). Save as Hive ORC table.
  3. Join weather facts with NDVI dimension, engineer lag features,
     and write the combined feature table to PostgreSQL for ML.

Storage systems used:
  - HDFS: raw satellite and weather CSVs (input)
  - HBase: NDVI time-series by region key (NoSQL random access)
  - Hive: weather indices ORC table (batch SQL analytics)
  - PostgreSQL: engineered features (RDBMS for ML and dashboards)
"""

import os
import sys
import glob
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)
from pyspark.sql.window import Window
import happybase

# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────

HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
HBASE_HOST = os.getenv("HBASE_HOST", "hbase")
POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://postgres:postgres@postgres:5432/crop_yield_db")

# GeoTIFF processing settings
MODIS_DIR = os.getenv("MODIS_DIR", "data/raw/modis")
GADM_DIR = os.getenv("GADM_DIR", "data/raw/gadm")
USE_GEOTIFF = os.getenv("USE_GEOTIFF", "auto")  # "auto", "yes", or "no"


# ──────────────────────────────────────────────────────────────────────
# Spark Session
# ──────────────────────────────────────────────────────────────────────

def init_spark():
    """Initialize PySpark session with Hive support."""
    print("--- Initializing PySpark Session with Hive Support ---")
    spark = SparkSession.builder \
        .appName("ClimateSmartAgriculture-ETL") \
        .config("spark.sql.warehouse.dir", f"{HDFS_NAMENODE}/user/hive/warehouse") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.driver.extraJavaOptions",
                "--add-opens=java.base/java.lang=ALL-UNNAMED "
                "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
                "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
                "--add-opens=java.base/java.io=ALL-UNNAMED "
                "--add-opens=java.base/java.net=ALL-UNNAMED "
                "--add-opens=java.base/java.nio=ALL-UNNAMED "
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                "--add-opens=java.base/java.util=ALL-UNNAMED "
                "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
                "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
                "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
                "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
                "--add-opens=java.security.jgss/sun.security.jgss=ALL-UNNAMED") \
        .config("spark.executor.extraJavaOptions",
                "--add-opens=java.base/java.lang=ALL-UNNAMED "
                "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
                "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
                "--add-opens=java.base/java.io=ALL-UNNAMED "
                "--add-opens=java.base/java.net=ALL-UNNAMED "
                "--add-opens=java.base/java.nio=ALL-UNNAMED "
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                "--add-opens=java.base/java.util=ALL-UNNAMED "
                "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
                "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
                "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
                "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
                "--add-opens=java.security.jgss/sun.security.jgss=ALL-UNNAMED") \
        .getOrCreate()
    return spark


# ──────────────────────────────────────────────────────────────────────
# HBase Setup & Writers
# ──────────────────────────────────────────────────────────────────────

def setup_hbase_table():
    """Create/recreate the HBase table for NDVI time-series storage."""
    print("--- Setting up HBase Table via Thrift Gateway ---")
    try:
        connection = happybase.Connection(host=HBASE_HOST, port=9090)
        connection.open()

        tables = connection.tables()
        if b'ndvi_time_series' in tables:
            print("HBase table 'ndvi_time_series' already exists. Recreating it...")
            connection.disable_table(b'ndvi_time_series')
            connection.delete_table(b'ndvi_time_series')

        connection.create_table(
            b'ndvi_time_series',
            {'info': dict(max_versions=1)}
        )
        print("HBase table 'ndvi_time_series' created successfully.")
        connection.close()
    except Exception as e:
        print("HBase connection/setup failed. Is HBase running?", e)


def write_to_hbase(partition):
    """Write NDVI records to HBase in parallel (runs on Spark workers)."""
    connection = happybase.Connection(
        host=os.getenv("HBASE_HOST", "hbase"), port=9090
    )
    connection.open()
    table = connection.table(b'ndvi_time_series')

    with table.batch(batch_size=1000) as b:
        for row in partition:
            cid = row['county_id']
            date_str = row['date']
            ndvi_val = row['avg_ndvi']

            # Row key: {county_id}_{date} → enables prefix scans per region
            row_key = f"{cid}_{date_str}".encode('utf-8')
            b.put(row_key, {
                b'info:ndvi': str(ndvi_val).encode('utf-8')
            })

    connection.close()


# ──────────────────────────────────────────────────────────────────────
# GeoTIFF Raster Processing (if MODIS .tif files are available)
# ──────────────────────────────────────────────────────────────────────

def process_geotiff_to_csv():
    """
    Process MODIS GeoTIFF files into zonal NDVI stats.
    Uses rasterio + rasterstats to compute mean NDVI per region per date.
    Results are written to a CSV that is then uploaded to HDFS.
    """
    if os.path.exists("data/satellite_ndvi_pixels.csv"):
        print("  satellite_ndvi_pixels.csv already exists. Skipping pre-processing.")
        return True
    try:
        import rasterio
        from rasterstats import zonal_stats
        import geopandas as gpd
        import numpy as np
        import re
    except ImportError:
        print("  rasterio/rasterstats/geopandas not available. Skipping GeoTIFF processing.")
        return False

    tif_files = glob.glob(os.path.join(MODIS_DIR, "**/*.tif"), recursive=True)
    if not tif_files:
        print(f"  No GeoTIFF files found in {MODIS_DIR}")
        return False

    print(f"  Found {len(tif_files)} GeoTIFF files. Processing rasters...")

    # Load region polygons from GADM shapefiles
    zones_gdf = _load_gadm_zones()
    if zones_gdf is None or zones_gdf.empty:
        print("  ⚠ No zone polygons available for zonal stats.")
        return False

    records = []
    for i, tif_path in enumerate(tif_files):
        try:
            # Parse date from MODIS filename
            basename = os.path.basename(tif_path)
            match = re.search(r"A(\d{4})(\d{3})", basename)
            if match:
                year = int(match.group(1))
                doy = int(match.group(2))
                from datetime import datetime, timedelta
                dt = datetime(year, 1, 1) + timedelta(days=doy - 1)
                date_str = dt.strftime("%Y-%m-%d")
            else:
                match2 = re.search(r"(\d{4})-?(\d{2})-?(\d{2})", basename)
                if match2:
                    date_str = f"{match2.group(1)}-{match2.group(2)}-{match2.group(3)}"
                else:
                    continue

            with rasterio.open(tif_path) as src:
                zones = zones_gdf.to_crs(src.crs)
                ndvi_data = src.read(1).astype(float)

                # Apply MODIS scale factor and nodata handling
                nodata = src.nodata
                if nodata is not None:
                    ndvi_data[ndvi_data == nodata] = np.nan
                # MODIS MOD13Q1 NDVI is scaled by 10000
                ndvi_data = ndvi_data / 10000.0

                stats = zonal_stats(
                    zones, ndvi_data, affine=src.transform,
                    stats=["mean", "max", "min"],
                    nodata=np.nan,
                )

                for j, stat in enumerate(stats):
                    if stat.get("mean") is not None:
                        records.append({
                            "county_id": int(zones.iloc[j]["county_id"]),
                            "date": date_str,
                            "pixel_x": 0,
                            "pixel_y": 0,
                            "ndvi": round(stat["mean"], 4),
                        })

            if (i + 1) % 10 == 0:
                print(f"    Processed {i + 1}/{len(tif_files)} rasters...")

        except Exception as e:
            print(f"    ⚠ Error on {tif_path}: {e}")

    if records:
        import pandas as pd
        df = pd.DataFrame(records)
        out_path = "data/satellite_ndvi_pixels.csv"
        df.to_csv(out_path, index=False)
        print(f"  ✓ Extracted {len(records):,} zonal NDVI records → {out_path}")
        return True

    return False


def _load_gadm_zones():
    """Load GADM admin level 1 boundaries as a GeoDataFrame."""
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import Polygon

    # Region ID mapping
    from data_ingest import TARGET_REGIONS

    # Approximate centroids for known agricultural regions
    APPROX_COORDS = {
        # US states
        "Iowa": (-93.5, 42.0), "Illinois": (-89.4, 40.0), "Indiana": (-86.3, 39.8),
        "Nebraska": (-99.8, 41.5), "Kansas": (-98.5, 38.5),
        # India states
        "Punjab": (75.3, 31.0), "Madhya Pradesh": (78.0, 23.5),
        "Maharashtra": (75.7, 19.7), "Uttar Pradesh": (80.9, 27.0), "Rajasthan": (73.5, 27.0),
        # Brazil states
        "Mato Grosso": (-55.0, -12.5), "Goiás": (-49.5, -15.9),
        "Paraná": (-51.5, -24.5), "São Paulo": (-48.5, -22.5), "Minas Gerais": (-44.5, -18.5),
        # China provinces
        "Henan": (113.5, 34.0), "Shandong": (117.0, 36.5),
        "Heilongjiang": (127.0, 47.0), "Jiangsu": (119.5, 33.0), "Anhui": (117.0, 32.0),
        # Kenya counties
        "Uasin Gishu": (35.3, 0.5), "Trans Nzoia": (34.9, 1.0),
        "Nakuru": (36.1, -0.3), "Nyandarua": (36.4, -0.2), "Bungoma": (34.6, 0.6),
    }

    all_gdf_parts = []
    for iso3, regions in TARGET_REGIONS.items():
        shp_path = os.path.join(GADM_DIR, iso3, f"gadm41_{iso3}_1.shp")
        use_fallback = False

        if not os.path.exists(shp_path):
            use_fallback = True
        else:
            try:
                gdf = gpd.read_file(shp_path).to_crs(epsg=4326)
                for cid, name in regions.items():
                    match = gdf[gdf["NAME_1"].str.contains(name, case=False, na=False)]
                    if not match.empty:
                        row = match.iloc[0:1].copy()
                        row["county_id"] = cid
                        all_gdf_parts.append(row[["county_id", "geometry"]])
                    else:
                        use_fallback = True
            except Exception as e:
                print(f"  ⚠ Failed to read GADM shapefile for {iso3} in pipeline: {e}. Using fallback coordinates.")
                use_fallback = True

        if use_fallback:
            # Generate fallback polygons
            fallback_records = []
            for cid, name in regions.items():
                lon, lat = APPROX_COORDS.get(name, (0.0, 0.0))
                half = 0.5
                coords = [
                    (lon - half, lat + half), (lon + half, lat + half),
                    (lon + half, lat - half), (lon - half, lat - half),
                    (lon - half, lat + half),
                ]
                geom = Polygon(coords)
                fallback_records.append({
                    "county_id": cid,
                    "geometry": geom
                })
            fallback_gdf = gpd.GeoDataFrame(fallback_records, crs="EPSG:4326")
            all_gdf_parts.append(fallback_gdf)

    if all_gdf_parts:
        return gpd.GeoDataFrame(pd.concat(all_gdf_parts, ignore_index=True), crs="EPSG:4326")
    return None


# ──────────────────────────────────────────────────────────────────────
# Pipeline Steps
# ──────────────────────────────────────────────────────────────────────

def run_pipeline(spark):
    """Execute the full ETL pipeline."""

    # ── Step 0: Optional GeoTIFF pre-processing ──
    if USE_GEOTIFF != "no":
        tif_files = glob.glob(os.path.join(MODIS_DIR, "**/*.tif"), recursive=True)
        if tif_files and USE_GEOTIFF in ("auto", "yes"):
            print("--- 0. Pre-processing MODIS GeoTIFF Rasters ---")
            process_geotiff_to_csv()

    # ── Step 1: Satellite NDVI Zonal Statistics ──
    print("--- 1. Reading and Aggregating Satellite NDVI (Zonal Stats) ---")
    ndvi_path = f"{HDFS_NAMENODE}/data/satellite/satellite_ndvi_pixels.csv"
    print(f"Reading satellite NDVI data from {ndvi_path}")
    sat_df = spark.read.csv(ndvi_path, header=True, inferSchema=True)

    # Zonal statistics: mean NDVI per county per composite date
    zonal_ndvi_df = sat_df.groupBy("county_id", "date") \
        .agg(F.round(F.mean("ndvi"), 4).alias("avg_ndvi")) \
        .orderBy("county_id", "date")

    zonal_ndvi_df.show(5)
    print(f"Total NDVI aggregates: {zonal_ndvi_df.count()}")

    # Write NDVI time-series to HBase (NoSQL — fast point lookups)
    print("Writing aggregated NDVI time series to HBase...")
    zonal_ndvi_df.foreachPartition(write_to_hbase)
    print("NDVI writing to HBase completed successfully.")

    # ── Step 2: Weather Observations Cleaning & Resampling ──
    print("--- 2. Cleaning and Resampling NOAA Weather Observations ---")
    weather_path = f"{HDFS_NAMENODE}/data/weather/weather_observations.csv"
    print(f"Reading weather observations from {weather_path}")
    weather_df = spark.read.csv(weather_path, header=True, inferSchema=True)

    # Clean: bound temperature, fix negative precipitation
    cleaned_weather = weather_df \
        .withColumn("temp_c",
                    F.when(F.col("temp_c").between(-40, 55), F.col("temp_c"))
                    .otherwise(F.lit(20.0))) \
        .withColumn("precip_mm",
                    F.when(F.col("precip_mm") >= 0, F.col("precip_mm"))
                    .otherwise(F.lit(0.0)))

    # Resample daily data to 16-day intervals (matching MODIS revisit)
    # Growing Degree Days (GDD) with base temperature 10°C
    resampled_weather = cleaned_weather \
        .withColumn("date_parsed",
                    F.to_date(F.col("date"), "yyyy-MM-dd")) \
        .withColumn("year_start",
                    F.trunc(F.col("date_parsed"), "year")) \
        .withColumn("days_since_year_start",
                    F.datediff(F.col("date_parsed"), F.col("year_start"))) \
        .withColumn("interval_id",
                    F.floor(F.col("days_since_year_start") / 16)) \
        .withColumn("resample_date",
                    F.expr("date_add(year_start, cast(interval_id * 16 as int))"))

    # Aggregate: seasonal climate indices per 16-day window
    weather_indices = resampled_weather.groupBy("county_id", "resample_date") \
        .agg(
            F.round(F.mean("temp_c"), 2).alias("avg_temp"),
            F.round(F.sum("precip_mm"), 2).alias("total_precip"),
            F.round(F.sum(
                F.when(F.col("temp_c") > 10, F.col("temp_c") - 10)
                .otherwise(0.0)
            ), 2).alias("gdd"),
        ) \
        .withColumnRenamed("resample_date", "date") \
        .orderBy("county_id", "date")

    weather_indices.show(5)

    # Write to Hive warehouse as ORC table (batch analytics store)
    print("Writing cleaned weather indices to Hive as ORC table...")
    spark.sql("DROP TABLE IF EXISTS weather_indices")
    weather_indices.write \
        .format("orc") \
        .mode("overwrite") \
        .option("path", f"{HDFS_NAMENODE}/user/hive/warehouse/weather_indices") \
        .saveAsTable("weather_indices")
    print("Hive ORC table 'weather_indices' written successfully.")

    # ── Step 3: Join Weather + NDVI → Feature Engineering ──
    print("--- 3. Joining Weather Facts and NDVI Dimension & Creating Features ---")
    weather_hive_df = spark.table("weather_indices")

    zonal_ndvi_df.createOrReplaceTempView("zonal_ndvi")
    weather_hive_df.createOrReplaceTempView("weather_hive")

    # Spark SQL join
    joined_sql_df = spark.sql("""
        SELECT
            w.county_id,
            w.date,
            w.avg_temp,
            w.total_precip,
            w.gdd,
            n.avg_ndvi
        FROM weather_hive w
        JOIN zonal_ndvi n
          ON w.county_id = n.county_id AND w.date = n.date
    """)

    # Lag features (temporal context for ML)
    windowSpec = Window.partitionBy("county_id").orderBy("date")
    featured_df = joined_sql_df \
        .withColumn("ndvi_lag1", F.lag("avg_ndvi", 1).over(windowSpec)) \
        .withColumn("ndvi_lag2", F.lag("avg_ndvi", 2).over(windowSpec)) \
        .withColumn("precip_lag1", F.lag("total_precip", 1).over(windowSpec)) \
        .withColumn("temp_lag1", F.lag("avg_temp", 1).over(windowSpec)) \
        .na.fill(0.0)

    print("Engineered features structure:")
    featured_df.show(5)

    # Write to PostgreSQL via JDBC (RDBMS — structured reporting)
    print("Saving joined and engineered features to PostgreSQL...")
    featured_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/crop_yield_db") \
        .option("dbtable", "yield_features") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print("Pipeline executed successfully and data written to all database layers.")


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    setup_hbase_table()
    spark = init_spark()
    run_pipeline(spark)
    spark.stop()
