import time
import geopandas as gpd
from shapely.wkt import loads
import rasterio
from rasterstats import zonal_stats
import numpy as np
import psycopg2

POSTGRES_URI = "postgresql://postgres:postgres@postgres:5432/crop_yield_db"

conn = psycopg2.connect(POSTGRES_URI)
cursor = conn.cursor()
cursor.execute("SELECT county_id, name, ST_AsText(geom) FROM counties")
rows = cursor.fetchall()

all_regions = []
for cid, name, geom_wkt in rows:
    geom = loads(geom_wkt)
    all_regions.append({"county_id": cid, "name": name, "geometry": geom})

gdf = gpd.GeoDataFrame(all_regions, crs="EPSG:4326")
tif_path = "data/raw/modis/MOD13Q1.A2024337.h10v05.061.2024358224020.tif"

for tol in [0.001, 0.005, 0.01, 0.02]:
    print(f"\nProfiling with tolerance={tol}...")
    start = time.time()
    # simplify
    gdf_simplified = gdf.copy()
    gdf_simplified["geometry"] = gdf.geometry.simplify(tolerance=tol, preserve_topology=True)
    
    with rasterio.open(tif_path) as src:
        zones = gdf_simplified.to_crs(src.crs)
        affine = src.transform
        ndvi_data = src.read(1).astype(float)
        ndvi_data = ndvi_data / 10000.0
        stats = zonal_stats(zones, ndvi_data, affine=affine, stats=["mean", "max", "min"], nodata=np.nan)
    print(f"Time taken with tol={tol}: {time.time() - start:.3f} seconds")
