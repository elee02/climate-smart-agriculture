#!/bin/bash
set -e

# Style colors
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m' # No Color

echo -e "${CYAN}=========================================================================${NC}"
echo -e "${GREEN}      CLIMATE-SMART AGRICULTURE: BIG DATA PIPELINE & PREDICTION          ${NC}"
echo -e "${GREEN}      Real-World Data • Polyglot Persistence • Spark MLlib                ${NC}"
echo -e "${CYAN}=========================================================================${NC}"
echo -e "${CYAN}  Countries: United States (5 agricultural regions)                      ${NC}"
echo -e "${CYAN}  Crops:     Maize (Corn), Wheat, Soybeans                                 ${NC}"
echo -e "${CYAN}  Data:      FAOSTAT, NOAA GSOD, MODIS NDVI, GADM Boundaries              ${NC}"
echo -e "${CYAN}=========================================================================${NC}"

# ──────────────────────────────────────────────────────────────────────
# Step 0: Download real data (optional — skip if data already exists)
# ──────────────────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[Step 0/9] Checking for downloaded data...${NC}"
if [ -f "data/raw/fao/fao_crop_production.csv" ] && [ -d "data/raw/gadm" ]; then
    echo -e "${GREEN}Real data already downloaded. Skipping download step.${NC}"
else
    echo -e "${YELLOW}Downloading real-world datasets (FAOSTAT, NOAA, GADM)...${NC}"
    echo "Note: MODIS GeoTIFF files can be downloaded using download_modis_real.py (requires NASA Earthdata Login)."
    python3 download_data.py --source fao --years 2015 2019 || echo -e "${RED}FAO download failed, will use fallback data.${NC}"
    python3 download_data.py --source noaa --years 2015 2019 || echo -e "${RED}NOAA download failed, will use fallback data.${NC}"
    python3 download_data.py --source gadm || echo -e "${RED}GADM download failed, will use fallback boundaries.${NC}"
fi

# ──────────────────────────────────────────────────────────────────────
# Step 1: Initialize Docker infrastructure
# ──────────────────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[Step 1/9] Initializing Docker Infrastructure...${NC}"
# Use plain down instead of down -v to preserve volumes across runs
docker-compose down || true
docker-compose up --build -d

# ──────────────────────────────────────────────────────────────────────
# Step 2: Wait for services
# ──────────────────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[Step 2/9] Waiting for Big Data services (HDFS, HBase, Postgres, Mongo) to boot...${NC}"
echo "Sleeping for 40 seconds to allow services to start and run health checks..."
for i in {40..1}; do
    echo -ne "Time remaining: $i seconds... \r"
    sleep 1
done
echo -e "\n${GREEN}Services should now be active!${NC}"

# ──────────────────────────────────────────────────────────────────────
# Step 3: Data ingestion (databases + local CSV processing)
# ──────────────────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[Step 3/9] Running Data Ingestion (GADM → PostGIS, FAO → Postgres, NOAA → CSV, MODIS → CSV)...${NC}"
docker-compose exec -T app python data_ingest.py

echo -e "\n${YELLOW}Uploading Weather & Satellite Data to HDFS...${NC}"
docker exec -i namenode hdfs dfs -mkdir -p /data/weather || true
docker exec -i namenode hdfs dfs -mkdir -p /data/satellite || true
docker exec -i namenode hdfs dfs -mkdir -p /data/streaming/weather_incoming || true

# Copy data files into namenode container, then into HDFS (always overwrite to ensure latest data is used)
if [ -f "data/weather_observations.csv" ]; then
    echo -e "${YELLOW}Uploading latest weather observations to HDFS...${NC}"
    docker cp data/weather_observations.csv namenode:/tmp/weather_observations.csv
    docker exec -i namenode hdfs dfs -put -f /tmp/weather_observations.csv /data/weather/weather_observations.csv
    docker exec -i namenode rm /tmp/weather_observations.csv
    echo -e "${GREEN}Weather data uploaded to HDFS.${NC}"
fi

if [ -f "data/satellite_ndvi_pixels.csv" ]; then
    echo -e "${YELLOW}Uploading latest satellite NDVI pixels to HDFS...${NC}"
    docker cp data/satellite_ndvi_pixels.csv namenode:/tmp/satellite_ndvi_pixels.csv
    docker exec -i namenode hdfs dfs -put -f /tmp/satellite_ndvi_pixels.csv /data/satellite/satellite_ndvi_pixels.csv
    docker exec -i namenode rm /tmp/satellite_ndvi_pixels.csv
    echo -e "${GREEN}Satellite data uploaded to HDFS.${NC}"
fi

# Upload MODIS GeoTIFF files to HDFS if they exist
MODIS_COUNT=$(find data/raw/modis -name "*.tif" 2>/dev/null | wc -l)
if [ "$MODIS_COUNT" -gt 0 ]; then
    # Check if files already exist in HDFS
    HDFS_MODIS_COUNT=$(docker exec -i namenode hdfs dfs -ls /data/satellite/modis 2>/dev/null | grep -c "\.tif$" || true)
    if [ "$HDFS_MODIS_COUNT" -ge "$MODIS_COUNT" ]; then
        echo -e "${GREEN}All ${MODIS_COUNT} MODIS GeoTIFF files already exist in HDFS. Skipping upload.${NC}"
    else
        echo -e "${YELLOW}Uploading ${MODIS_COUNT} MODIS GeoTIFF files to HDFS in a single batch...${NC}"
        docker exec -i namenode hdfs dfs -mkdir -p /data/satellite/modis || true
        # Clean up any partial state in container first
        docker exec -i namenode rm -rf /tmp/modis || true
        # Copy the entire directory at once
        docker cp data/raw/modis namenode:/tmp/modis
        # Put the whole folder at once to HDFS (this will be extremely fast)
        docker exec -i namenode sh -c "hdfs dfs -put -f /tmp/modis/*.tif /data/satellite/modis/"
        # Clean up temp folder in container
        docker exec -i namenode rm -rf /tmp/modis
        echo -e "${GREEN}MODIS GeoTIFF files uploaded to HDFS.${NC}"
    fi
fi

echo -e "${GREEN}HDFS uploads complete!${NC}"

# ──────────────────────────────────────────────────────────────────────
# Step 4: PostgreSQL → HDFS export (Sqoop-style ingest)
# ──────────────────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[Step 4/9] Exporting Crop Yields from PostgreSQL → HDFS Warehouse...${NC}"
docker-compose exec -T app python sqoop_ingest.py import \
  --connect jdbc:postgresql://postgres:5432/crop_yield_db \
  --table crop_yields \
  --target-dir /user/hive/warehouse/crop_yields \
  --username postgres \
  --password postgres

# ──────────────────────────────────────────────────────────────────────
# Step 5: PySpark ETL pipeline
# ──────────────────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[Step 5/9] Submitting PySpark ETL Job (Zonal Stats, HBase write, Hive ORC weather)...${NC}"
docker-compose exec -T app spark-submit \
  --packages org.postgresql:postgresql:42.6.0 \
  --master spark://spark:7077 \
  pipeline.py

# ──────────────────────────────────────────────────────────────────────
# Step 6: Flume simulation
# ──────────────────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[Step 6/9] Running Flume Ingestion Agent Simulation (30 seconds)...${NC}"
docker-compose exec -T app python flume_agent.py --mode demo --duration 30 --rate 5

# ──────────────────────────────────────────────────────────────────────
# Step 7: Spark Structured Streaming
# ──────────────────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[Step 7/9] Running Spark Structured Streaming Demo (60 seconds)...${NC}"
docker-compose exec -T app spark-submit \
  --master spark://spark:7077 \
  spark_streaming.py --duration 60 --trigger-interval 15

# ──────────────────────────────────────────────────────────────────────
# Step 8: Spark MLlib with cross-validation
# ──────────────────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[Step 8/9] Checking if Spark MLlib Model Training can be skipped...${NC}"
INPUTS_HASH=""
if [ -f "data/satellite_ndvi_pixels.csv" ] && [ -f "data/weather_observations.csv" ] && [ -f "data/crop_yields.csv" ]; then
    INPUTS_HASH=$(md5sum data/satellite_ndvi_pixels.csv data/weather_observations.csv data/crop_yields.csv 2>/dev/null | md5sum | awk '{print $1}')
fi

SKIP_TRAIN=false
if [ -n "$INPUTS_HASH" ] && [ -f "data/.model_hash" ] && [ "$(cat data/.model_hash)" = "$INPUTS_HASH" ]; then
    # Verify that yield_predictions table is populated in PostgreSQL
    PRED_COUNT=$(docker exec -i postgres-db psql -U postgres -d crop_yield_db -t -c "SELECT COUNT(*) FROM yield_predictions;" 2>/dev/null | tr -d '[:space:]')
    if echo "$PRED_COUNT" | grep -q '^[0-9][0-9]*$' 2>/dev/null && [ "$PRED_COUNT" -gt 0 ]; then
        echo -e "${GREEN}Model inputs have not changed and predictions exist. Skipping Spark MLlib model training.${NC}"
        SKIP_TRAIN=true
    fi
fi

if [ "$SKIP_TRAIN" = false ]; then
    echo -e "${YELLOW}Submitting Spark MLlib Job (Random Forest + 5-Fold Cross-Validation)...${NC}"
    docker-compose exec -T app spark-submit \
      --packages org.postgresql:postgresql:42.6.0 \
      --master spark://spark:7077 \
      ml_model.py
    if [ -n "$INPUTS_HASH" ]; then
        echo "$INPUTS_HASH" > data/.model_hash
    fi
fi

# ──────────────────────────────────────────────────────────────────────
# Step 9: Performance benchmarks
# ──────────────────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[Step 9/9] Running Performance Benchmarks (HBase vs PostgreSQL vs Hive + Scaling)...${NC}"
docker-compose exec -T app python benchmark.py

# ──────────────────────────────────────────────────────────────────────
# Step 10: Start persistent background streaming pipeline
# ──────────────────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[Step 10/10] Starting persistent background streaming pipeline...${NC}"

# Function to check if process is running inside container
check_running_in_container() {
    docker-compose exec -T app python3 -c "
import os, sys
name = sys.argv[1]
mypid = os.getpid()
myppid = os.getppid()
for p in os.listdir('/proc'):
    if p.isdigit():
        try:
            val = int(p)
            if val == mypid or val == myppid:
                continue
            with open(f'/proc/{p}/cmdline', 'r') as f:
                if name in f.read():
                    sys.exit(0)
        except Exception:
            pass
sys.exit(1)
" "$1"
}

if check_running_in_container "flume_agent.py"; then
    echo -e "${GREEN}Flume Agent is already running in the background.${NC}"
else
    docker-compose exec -d app sh -c "python -u flume_agent.py --mode demo --rate 1800 --duration 999999 > /app/data/flume_agent.log 2>&1"
    echo -e "${GREEN}Started background Flume Agent.${NC}"
fi

if check_running_in_container "spark_streaming.py"; then
    echo -e "${GREEN}Spark Structured Streaming is already running in the background.${NC}"
else
    docker-compose exec -d app sh -c "spark-submit --master spark://spark:7077 spark_streaming.py --duration 999999 --trigger-interval 30 > /app/data/spark_streaming.log 2>&1"
    echo -e "${GREEN}Started background Spark Structured Streaming.${NC}"
fi

echo -e "${GREEN}Background streaming processes active. Logs available at data/flume_agent.log and data/spark_streaming.log${NC}"

# ──────────────────────────────────────────────────────────────────────
# Done
# ──────────────────────────────────────────────────────────────────────
echo -e "\n${GREEN}=========================================================================${NC}"
echo -e "${GREEN}               PIPELINE RUN COMPLETED SUCCESSFULLY!                      ${NC}"
echo -e "${CYAN}=========================================================================${NC}"
echo -e "Dashboard:          ${CYAN}http://localhost:5000${NC}"
echo -e "HBase Web UI:       ${CYAN}http://localhost:16010${NC}"
echo -e "HDFS Web UI:        ${CYAN}http://localhost:9870${NC}"
echo -e "Spark Web UI:       ${CYAN}http://localhost:8080${NC}"
echo -e "${CYAN}=========================================================================${NC}"
echo -e "Tailing application server logs (Ctrl+C to exit)..."
docker-compose logs -f app
