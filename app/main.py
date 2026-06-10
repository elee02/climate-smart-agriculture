#!/usr/bin/env python3
"""
Climate-Smart Agriculture — Flask Web Dashboard

API endpoints serving data from the polyglot database layer:
  - /api/geojson         — County boundaries from MongoDB (GeoJSON)
  - /api/predictions     — Yield predictions from PostgreSQL
  - /api/benchmarks      — Performance benchmark results from MongoDB
  - /api/feature-importance — ML feature importances from MongoDB
  - /api/ndvi/<county_id> — NDVI time-series from HBase
  - /api/cv-results      — Cross-validation results from MongoDB
  - /api/scaling         — Scaling benchmark results from MongoDB
  - /api/regions         — Region metadata from PostgreSQL
"""

import os
import sys
from flask import Flask, jsonify, render_template
from pymongo import MongoClient
import happybase
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

# Load configurations from environment variables
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://postgres:postgres@localhost:5432/crop_yield_db")
HBASE_HOST = os.getenv("HBASE_HOST", "localhost")


# ── Database connections ──

def get_mongo_db():
    client = MongoClient(MONGO_URI)
    return client["crop_dashboard"]

def get_pg_conn():
    return psycopg2.connect(POSTGRES_URI, cursor_factory=RealDictCursor)


# ── Routes ──

@app.route('/')
def index():
    return render_template("index.html")


@app.route('/api/geojson')
def get_geojson():
    try:
        db = get_mongo_db()
        counties = list(db["counties"].find({}, {"_id": 0}))
        feature_collection = {
            "type": "FeatureCollection",
            "features": counties,
        }
        return jsonify(feature_collection)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/predictions')
def get_predictions():
    try:
        conn = get_pg_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT p.*, c.name as county_name, c.country
            FROM yield_predictions p
            JOIN counties c ON p.county_id = c.county_id
            ORDER BY p.year ASC, p.county_id ASC
        """)
        rows = cursor.fetchall()
        conn.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/benchmarks')
def get_benchmarks():
    try:
        db = get_mongo_db()
        benchmarks = list(db["benchmarks"].find({}, {"_id": 0}))
        return jsonify(benchmarks)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/feature-importance')
def get_feature_importance():
    try:
        db = get_mongo_db()
        importances = list(db["feature_importance"].find({}, {"_id": 0}))
        return jsonify(importances)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/ndvi/<int:county_id>')
def get_ndvi_time_series(county_id):
    """Fetch NDVI time-series from HBase by county ID (NoSQL point lookup)."""
    print(f"Fetching NDVI time-series scan from HBase for county {county_id}...")
    try:
        connection = happybase.Connection(host=HBASE_HOST, port=9090)
        connection.open()
        table = connection.table(b'ndvi_time_series')

        prefix = f"{county_id}_".encode('utf-8')
        results = []

        for key, data in table.scan(row_prefix=prefix):
            row_key_str = key.decode('utf-8')
            parts = row_key_str.split('_', 1)
            if len(parts) == 2:
                date_str = parts[1]
                ndvi_val = float(data[b'info:ndvi'].decode('utf-8'))
                results.append({
                    "date": date_str,
                    "ndvi": ndvi_val,
                })

        connection.close()

        results.sort(key=lambda x: x["date"])
        return jsonify({"county_id": county_id, "data": results})
    except Exception as e:
        print("HBase query failed:", e)
        return jsonify({"error": str(e)}), 500


@app.route('/api/weather/<int:county_id>')
def get_weather_stream(county_id):
    """Fetch real-time weather observations from HBase by county ID."""
    print(f"Fetching real-time weather scan from HBase for county {county_id}...")
    try:
        connection = happybase.Connection(host=HBASE_HOST, port=9090)
        connection.open()
        table = connection.table(b'weather_stream')

        prefix = f"{county_id}_".encode('utf-8')
        results = []

        for key, data in table.scan(row_prefix=prefix):
            row_key_str = key.decode('utf-8')
            parts = row_key_str.split('_', 1)
            if len(parts) == 2:
                date_str = parts[1]
                avg_temp = float(data[b'info:avg_temp'].decode('utf-8'))
                total_precip = float(data[b'info:total_precip'].decode('utf-8'))
                record_count = int(data[b'info:record_count'].decode('utf-8'))
                results.append({
                    "date": date_str,
                    "avg_temp": avg_temp,
                    "total_precip": total_precip,
                    "record_count": record_count,
                })

        connection.close()
        results.sort(key=lambda x: x["date"])
        return jsonify({"county_id": county_id, "data": results})
    except Exception as e:
        print("HBase weather query failed:", e)
        return jsonify({"error": str(e)}), 500


@app.route('/api/cv-results')
def get_cv_results():
    """Fetch cross-validation results from MongoDB."""
    try:
        db = get_mongo_db()
        results = list(db["cv_results"].find({}, {"_id": 0}))
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/scaling')
def get_scaling_results():
    """Fetch scaling benchmark results from MongoDB."""
    try:
        db = get_mongo_db()
        results = list(db["scaling_results"].find({}, {"_id": 0}))
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/format-comparison')
def get_format_comparison():
    """Fetch format comparison benchmark results from MongoDB."""
    try:
        db = get_mongo_db()
        result = db["format_comparison"].find_one({}, {"_id": 0})
        return jsonify(result or {})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/regions')
def get_regions():
    """Fetch region metadata from PostgreSQL."""
    try:
        conn = get_pg_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT county_id, name, country
            FROM counties
            ORDER BY county_id
        """)
        rows = cursor.fetchall()
        conn.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
