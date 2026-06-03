#!/usr/bin/env python3
"""
Climate-Smart Agriculture — Performance Benchmarks

Compares query performance across the polyglot storage systems:
  1. HBase (NoSQL) — point lookups by composite row key
  2. PostgreSQL (RDBMS) — indexed point queries
  3. Hive / Spark SQL — batch aggregation scans over ORC tables

Also includes:
  4. Scaling benchmark — same query over increasing data volumes
  5. Hive ORC vs raw CSV comparison

Results are saved to MongoDB for the dashboard.
"""

import os
import time
import json
import psycopg2
import happybase
from pymongo import MongoClient
from pyspark.sql import SparkSession

# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────

HBASE_HOST = os.getenv("HBASE_HOST", "localhost")
POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://postgres:postgres@localhost:5432/crop_yield_db")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")


# ──────────────────────────────────────────────────────────────────────
# Spark Session
# ──────────────────────────────────────────────────────────────────────

def get_spark():
    """Initialize Spark session for benchmark queries."""
    return SparkSession.builder \
        .appName("Benchmark") \
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


# ──────────────────────────────────────────────────────────────────────
# Benchmark 1: HBase Point Lookups
# ──────────────────────────────────────────────────────────────────────

def benchmark_hbase():
    """Benchmark HBase single-row Get operations (NoSQL point lookups)."""
    print("\n[Benchmark 1] HBase — Point Lookups")
    print("-" * 50)
    try:
        connection = happybase.Connection(host=HBASE_HOST, port=9090)
        connection.open()
        table = connection.table(b'ndvi_time_series')

        # Warmup
        table.row(b'1_2015-01-01')

        iterations = 100
        start_time = time.time()
        for i in range(iterations):
            row = table.row(f"1_2015-01-{(i % 28) + 1:02d}".encode())
        end_time = time.time()

        connection.close()
        avg_latency_ms = ((end_time - start_time) / iterations) * 1000.0
        print(f"  ✓ HBase Average Latency: {avg_latency_ms:.4f} ms ({iterations} iterations)")
        return avg_latency_ms
    except Exception as e:
        print(f"  ✗ HBase benchmark failed: {e}")
        return None


# ──────────────────────────────────────────────────────────────────────
# Benchmark 2: PostgreSQL Indexed Lookups
# ──────────────────────────────────────────────────────────────────────

def benchmark_postgres():
    """Benchmark PostgreSQL indexed point queries."""
    print("\n[Benchmark 2] PostgreSQL — Indexed Point Queries")
    print("-" * 50)
    try:
        conn = psycopg2.connect(POSTGRES_URI)
        cursor = conn.cursor()

        # Warmup
        cursor.execute("SELECT avg_ndvi FROM yield_features WHERE county_id = 1 AND date = '2015-01-01'")
        cursor.fetchall()

        iterations = 100
        start_time = time.time()
        for i in range(iterations):
            cursor.execute("SELECT avg_ndvi FROM yield_features WHERE county_id = 1 AND date = '2015-01-01'")
            cursor.fetchall()
        end_time = time.time()

        conn.close()
        avg_latency_ms = ((end_time - start_time) / iterations) * 1000.0
        print(f"  ✓ PostgreSQL Average Latency: {avg_latency_ms:.4f} ms ({iterations} iterations)")
        return avg_latency_ms
    except Exception as e:
        print(f"  ✗ PostgreSQL benchmark failed: {e}")
        return None


# ──────────────────────────────────────────────────────────────────────
# Benchmark 3: Hive / Spark SQL Batch Aggregation
# ──────────────────────────────────────────────────────────────────────

def benchmark_hive_spark():
    """Benchmark Hive/Spark SQL full-table aggregation scan on ORC data."""
    print("\n[Benchmark 3] Hive / Spark SQL — Batch Aggregation Scan")
    print("-" * 50)
    try:
        spark = get_spark()

        # Warmup
        spark.sql("SELECT mean(avg_temp) FROM weather_indices").collect()

        iterations = 5
        start_time = time.time()
        for i in range(iterations):
            df = spark.sql("""
                SELECT county_id,
                       mean(avg_temp) as avg_temp,
                       sum(total_precip) as total_precip,
                       sum(gdd) as total_gdd
                FROM weather_indices
                GROUP BY county_id
            """)
            df.collect()
        end_time = time.time()

        spark.stop()
        avg_latency_ms = ((end_time - start_time) / iterations) * 1000.0
        print(f"  ✓ Hive/Spark SQL Average Latency: {avg_latency_ms:.4f} ms ({iterations} iterations)")
        return avg_latency_ms
    except Exception as e:
        print(f"  ✗ Hive benchmark failed: {e}")
        return None


# ──────────────────────────────────────────────────────────────────────
# Benchmark 4: Scaling Analysis
# ──────────────────────────────────────────────────────────────────────

def benchmark_scaling():
    """
    Process increasing data volumes and measure execution time.
    Demonstrates sub-linear scaling behavior of distributed processing.
    """
    print("\n[Benchmark 4] Scaling Analysis — Variable Data Volume")
    print("-" * 50)

    scaling_results = []
    try:
        spark = get_spark()

        # Load the full weather CSV from HDFS
        weather_path = f"{HDFS_NAMENODE}/data/weather/weather_observations.csv"
        full_df = spark.read.csv(weather_path, header=True, inferSchema=True)
        full_count = full_df.count()
        print(f"  Full dataset: {full_count:,} rows")

        if full_count == 0:
            print("  ⚠ No data available for scaling benchmark.")
            spark.stop()
            return []

        # Test with different fractions of data
        fractions = [0.1, 0.25, 0.5, 0.75, 1.0]
        for frac in fractions:
            sample_df = full_df.sample(fraction=frac, seed=42)
            sample_count = sample_df.count()

            # Time the aggregation
            start = time.time()
            result = sample_df.groupBy("county_id") \
                .agg({"temp_c": "mean", "precip_mm": "sum"}) \
                .collect()
            elapsed_ms = (time.time() - start) * 1000.0

            scaling_results.append({
                "fraction": frac,
                "row_count": sample_count,
                "latency_ms": round(elapsed_ms, 2),
            })
            print(f"  {frac*100:5.0f}% ({sample_count:>8,} rows) → {elapsed_ms:>8.1f} ms")

        spark.stop()

        # Compute scaling factor (ideal linear = 1.0)
        if len(scaling_results) >= 2:
            base = scaling_results[0]
            full = scaling_results[-1]
            data_ratio = full["row_count"] / max(base["row_count"], 1)
            time_ratio = full["latency_ms"] / max(base["latency_ms"], 1)
            scaling_factor = time_ratio / data_ratio if data_ratio > 0 else 0
            print(f"\n  Scaling factor: {scaling_factor:.3f} (1.0 = linear, <1.0 = sub-linear)")
            scaling_results.append({
                "fraction": -1,
                "row_count": 0,
                "latency_ms": 0,
                "scaling_factor": round(scaling_factor, 3),
            })

    except Exception as e:
        print(f"  ✗ Scaling benchmark failed: {e}")

    return scaling_results


# ──────────────────────────────────────────────────────────────────────
# Benchmark 5: ORC vs CSV Format Comparison
# ──────────────────────────────────────────────────────────────────────

def benchmark_format_comparison():
    """Compare query performance on Hive ORC table vs raw CSV on HDFS."""
    print("\n[Benchmark 5] Format Comparison — ORC vs Raw CSV")
    print("-" * 50)

    results = {}
    try:
        spark = get_spark()

        # ORC (Hive) query
        start = time.time()
        for _ in range(3):
            spark.sql("SELECT county_id, sum(gdd), avg(avg_temp) FROM weather_indices GROUP BY county_id").collect()
        orc_ms = ((time.time() - start) / 3) * 1000.0

        # CSV (HDFS) query — same aggregation but from raw CSV
        weather_csv = f"{HDFS_NAMENODE}/data/weather/weather_observations.csv"
        csv_df = spark.read.csv(weather_csv, header=True, inferSchema=True)

        start = time.time()
        for _ in range(3):
            csv_df.groupBy("county_id") \
                .agg({"temp_c": "avg", "precip_mm": "sum"}) \
                .collect()
        csv_ms = ((time.time() - start) / 3) * 1000.0

        spark.stop()

        speedup = csv_ms / orc_ms if orc_ms > 0 else 0
        results = {
            "orc_latency_ms": round(orc_ms, 2),
            "csv_latency_ms": round(csv_ms, 2),
            "orc_speedup": round(speedup, 2),
        }
        print(f"  ORC (Hive):  {orc_ms:>8.1f} ms")
        print(f"  CSV (HDFS):  {csv_ms:>8.1f} ms")
        print(f"  ORC speedup: {speedup:.2f}x")

    except Exception as e:
        print(f"  ✗ Format comparison failed: {e}")

    return results


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

def main():
    print("\n╔════════════════════════════════════════════════════════════════╗")
    print("║  PERFORMANCE BENCHMARKS — SQL vs NoSQL vs Hive               ║")
    print("╚════════════════════════════════════════════════════════════════╝")

    time.sleep(2)  # Let databases settle

    # Core benchmarks
    hbase_latency = benchmark_hbase()
    postgres_latency = benchmark_postgres()
    hive_latency = benchmark_hive_spark()

    # Advanced benchmarks
    scaling_results = benchmark_scaling()
    format_results = benchmark_format_comparison()

    # ── Save all results to MongoDB ──
    all_results = {
        "core_benchmarks": [
            {"system": "HBase (NoSQL)", "query_type": "Point Get", "latency_ms": hbase_latency or 0.0},
            {"system": "PostgreSQL (RDBMS)", "query_type": "Indexed Point Query", "latency_ms": postgres_latency or 0.0},
            {"system": "Hive / Spark SQL", "query_type": "Batch Aggregation Scan", "latency_ms": hive_latency or 0.0},
        ],
        "scaling_analysis": scaling_results,
        "format_comparison": format_results,
    }

    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client["crop_dashboard"]

        # Save core benchmarks (backward compatible)
        db["benchmarks"].drop()
        db["benchmarks"].insert_many(all_results["core_benchmarks"])

        # Save advanced benchmarks
        db["scaling_results"].drop()
        if scaling_results:
            db["scaling_results"].insert_many(scaling_results)

        db["format_comparison"].drop()
        if format_results:
            db["format_comparison"].insert_one(format_results)

        print("\n✓ All benchmark results saved to MongoDB.")
    except Exception as e:
        print(f"\n✗ Failed to save benchmark results to MongoDB: {e}")

    # Summary
    print(f"\n{'='*60}")
    print("  BENCHMARK SUMMARY")
    print(f"{'='*60}")
    if hbase_latency:
        print(f"  HBase point lookup:       {hbase_latency:>10.4f} ms")
    if postgres_latency:
        print(f"  PostgreSQL indexed query:  {postgres_latency:>10.4f} ms")
    if hive_latency:
        print(f"  Hive/Spark SQL scan:       {hive_latency:>10.1f} ms")
    if format_results:
        print(f"  ORC vs CSV speedup:        {format_results.get('orc_speedup', 0):>10.2f}x")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
