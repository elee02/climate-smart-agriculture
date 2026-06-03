#!/usr/bin/env python3
"""
Climate-Smart Agriculture — Spark Structured Streaming Job

Monitors an HDFS directory for incoming weather observation CSV files
(produced by the Flume agent) and processes them in near-real-time:
  1. Reads new CSV files as a streaming DataFrame
  2. Cleans and validates incoming weather records
  3. Computes sliding-window aggregates (running averages, cumulative rainfall)
  4. Writes updated summaries to HBase and/or console output

This demonstrates Spark Structured Streaming for real-time climate monitoring.

Usage:
  # Run for 60 seconds monitoring HDFS streaming directory
  spark-submit --packages org.postgresql:postgresql:42.6.0 \
    spark_streaming.py --duration 60

  # Run with custom trigger interval
  spark-submit spark_streaming.py --trigger-interval 10 --duration 120
"""

import os
import sys
import time
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)
import happybase


# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────

HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
HBASE_HOST = os.getenv("HBASE_HOST", "hbase")
STREAMING_INPUT_DIR = "/data/streaming/weather_incoming"
CHECKPOINT_DIR = "/data/streaming/checkpoints/weather_stream"


# ──────────────────────────────────────────────────────────────────────
# Schema definition for incoming weather CSVs
# ──────────────────────────────────────────────────────────────────────

WEATHER_SCHEMA = StructType([
    StructField("county_id", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("temp_c", DoubleType(), True),
    StructField("precip_mm", DoubleType(), True),
])


# ──────────────────────────────────────────────────────────────────────
# HBase Writer for Streaming Results
# ──────────────────────────────────────────────────────────────────────

def setup_streaming_hbase_table():
    """Create HBase table for streaming weather summaries."""
    try:
        connection = happybase.Connection(host=HBASE_HOST, port=9090)
        connection.open()

        tables = connection.tables()
        if b'weather_stream' not in tables:
            connection.create_table(
                b'weather_stream',
                {'info': dict(max_versions=3)}
            )
            print("HBase table 'weather_stream' created.")
        else:
            print("HBase table 'weather_stream' already exists.")

        connection.close()
    except Exception as e:
        print(f"HBase setup warning: {e}")


def write_batch_to_hbase(batch_df, epoch_id):
    """Write each micro-batch to HBase (foreachBatch sink)."""
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()
    try:
        connection = happybase.Connection(host=HBASE_HOST, port=9090)
        connection.open()
        table = connection.table(b'weather_stream')

        with table.batch(batch_size=100) as b:
            for row in rows:
                row_key = f"{row['county_id']}_{row['date']}".encode('utf-8')
                b.put(row_key, {
                    b'info:avg_temp': str(row['avg_temp']).encode('utf-8'),
                    b'info:total_precip': str(row['total_precip']).encode('utf-8'),
                    b'info:record_count': str(row['record_count']).encode('utf-8'),
                    b'info:epoch': str(epoch_id).encode('utf-8'),
                })

        connection.close()
        print(f"  [Epoch {epoch_id}] Wrote {len(rows)} aggregates to HBase.")
    except Exception as e:
        print(f"  [Epoch {epoch_id}] HBase write failed: {e}")
        # Fall back to console output
        print(f"  [Epoch {epoch_id}] Batch contents ({len(rows)} rows):")
        batch_df.show(truncate=False)


# ──────────────────────────────────────────────────────────────────────
# Streaming Pipeline
# ──────────────────────────────────────────────────────────────────────

def run_streaming(duration=60, trigger_interval=30):
    """Run Spark Structured Streaming job."""
    print("\n╔════════════════════════════════════════════════════════════════╗")
    print("║  SPARK STRUCTURED STREAMING — Real-Time Weather Monitoring   ║")
    print("╠════════════════════════════════════════════════════════════════╣")
    print(f"║  Input:     {HDFS_NAMENODE}{STREAMING_INPUT_DIR:<30}║")
    print(f"║  Trigger:   Every {trigger_interval} seconds{' '*(38-len(str(trigger_interval)))}║")
    print(f"║  Duration:  {duration} seconds{' '*(42-len(str(duration)))}║")
    print("╚════════════════════════════════════════════════════════════════╝\n")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ClimateSmartAgriculture-Streaming") \
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

    # Setup HBase output table
    setup_streaming_hbase_table()

    # ── Read Stream ──
    # Monitor HDFS directory for new CSV files
    input_path = f"{HDFS_NAMENODE}{STREAMING_INPUT_DIR}"
    print(f"Monitoring: {input_path}")

    streaming_df = spark.readStream \
        .schema(WEATHER_SCHEMA) \
        .option("header", "true") \
        .option("maxFilesPerTrigger", 10) \
        .csv(input_path)

    # ── Transform ──
    # Clean incoming data
    cleaned_df = streaming_df \
        .filter(F.col("county_id").isNotNull()) \
        .filter(F.col("date").isNotNull()) \
        .withColumn("temp_c",
                    F.when(F.col("temp_c").between(-40, 55), F.col("temp_c"))
                    .otherwise(F.lit(None))) \
        .withColumn("precip_mm",
                    F.when(F.col("precip_mm") >= 0, F.col("precip_mm"))
                    .otherwise(F.lit(0.0)))

    # Aggregate per county per date
    aggregated_df = cleaned_df \
        .groupBy("county_id", "date") \
        .agg(
            F.round(F.avg("temp_c"), 2).alias("avg_temp"),
            F.round(F.sum("precip_mm"), 2).alias("total_precip"),
            F.count("*").alias("record_count"),
        )

    # ── Write Stream ──
    # Use foreachBatch to write to HBase
    query = aggregated_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_batch_to_hbase) \
        .option("checkpointLocation", f"{HDFS_NAMENODE}{CHECKPOINT_DIR}") \
        .trigger(processingTime=f"{trigger_interval} seconds") \
        .start()

    print(f"\nStreaming query started. Running for {duration} seconds...")
    print("(New files in the streaming directory will be processed automatically)\n")

    # Run for specified duration
    try:
        query.awaitTermination(timeout=duration)
    except Exception as e:
        print(f"Streaming terminated: {e}")
    finally:
        query.stop()
        spark.stop()

    print("\n✓ Structured Streaming job completed.")


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Spark Structured Streaming Weather Monitor")
    parser.add_argument("--duration", type=int, default=60,
                        help="How long to run the streaming job (seconds, default: 60)")
    parser.add_argument("--trigger-interval", type=int, default=30,
                        help="Processing trigger interval (seconds, default: 30)")
    args = parser.parse_args()

    run_streaming(duration=args.duration, trigger_interval=args.trigger_interval)


if __name__ == "__main__":
    main()
