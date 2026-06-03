import psycopg2
import json

conn = psycopg2.connect("dbname=climate user=el02 host=localhost")
cur = conn.cursor()
cur.execute("SELECT county_id, name, ST_Y(ST_Centroid(geom)) as lat, ST_X(ST_Centroid(geom)) as lon FROM counties ORDER BY county_id")
results = [{"county_id": r[0], "name": r[1], "lat": r[2], "lon": r[3]} for r in cur.fetchall()]
print(json.dumps(results, indent=2))
