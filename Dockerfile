FROM python:3.10-slim

# Install system dependencies:
#   - Java 21 for PySpark
#   - GDAL for rasterio/fiona geospatial processing
#   - gcc/g++ for native compilations
#   - libpq-dev for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    gcc \
    g++ \
    libpq-dev \
    curl \
    gdal-bin \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

# Set GDAL environment variables for rasterio/fiona builds
ENV GDAL_CONFIG=/usr/bin/gdal-config

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --timeout=120 --retries=10 -r requirements.txt

# Copy spark default configs for Java compatibility
COPY spark-defaults.conf /usr/local/lib/python3.10/site-packages/pyspark/conf/spark-defaults.conf

# Create project directories
RUN mkdir -p data/raw/fao data/raw/noaa data/raw/modis data/raw/gadm data/processed app/templates app/static

# Set default command
CMD ["python", "app/main.py"]
