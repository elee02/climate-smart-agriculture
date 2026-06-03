import os
import json
import psycopg2

POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://postgres:postgres@localhost:5432/crop_yield_db")

conn = None
cur = None
try:
    conn = psycopg2.connect(POSTGRES_URI)
    cur = conn.cursor()
    cur.execute("SELECT county_id, name, ST_Y(ST_Centroid(geom)) as lat, ST_X(ST_Centroid(geom)) as lon FROM counties ORDER BY county_id")
    results = [{"county_id": r[0], "name": r[1], "lat": r[2], "lon": r[3]} for r in cur.fetchall()]
    print(json.dumps(results, indent=2))
finally:
    if cur:
        cur.close()
    if conn:
        conn.close()
