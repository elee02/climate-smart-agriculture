#!/usr/bin/env python3
"""
Climate-Smart Agriculture — Real-World Data Download Script

Downloads open data from four sources:
  1. FAOSTAT  — Crop production statistics (bulk CSV, no auth)
  2. NOAA GSOD — Daily weather observations (AWS S3, no auth)
  3. NASA MODIS — Satellite NDVI GeoTIFF (requires free Earthdata Login)
  4. GADM — Administrative boundaries / shapefiles (no auth)

Usage:
  python download_data.py --source all          # download everything
  python download_data.py --source fao          # just FAO crop data
  python download_data.py --source noaa         # just NOAA weather
  python download_data.py --source modis        # MODIS download guide + helper
  python download_data.py --source gadm         # just GADM boundaries
  python download_data.py --source all --years 2020 2023   # custom year range
"""

import os
import sys
import argparse
import time
import zipfile
import io
import glob
import json
import requests
import pandas as pd
import numpy as np

# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────

# 5 major agricultural countries (limited to USA for all-real data pipeline)
TARGET_COUNTRIES = {
    "USA": {
        "name": "United States of America",
        "iso3": "USA",
        "noaa_fips": "US",
        "regions": ["Iowa", "Illinois", "Indiana", "Nebraska", "Kansas"],
    },
}

# Target crops (FAO item codes) - Rice dropped as it's not key for Midwest US
TARGET_CROPS = {
    "Maize (corn)": 56,
    "Wheat": 15,
    "Soybeans": 236,
}

# MODIS tile IDs — one representative tile per country
MODIS_TILES = {
    "USA": "h10v05",   # Iowa / Illinois
}

# Data directories
BASE_DATA_DIR = "data"
RAW_DIR = os.path.join(BASE_DATA_DIR, "raw")
FAO_DIR = os.path.join(RAW_DIR, "fao")
NOAA_DIR = os.path.join(RAW_DIR, "noaa")
MODIS_DIR = os.path.join(RAW_DIR, "modis")
GADM_DIR = os.path.join(RAW_DIR, "gadm")

# Max stations per country to avoid downloading thousands
MAX_STATIONS_PER_COUNTRY = 10


# ──────────────────────────────────────────────────────────────────────
# Helper functions
# ──────────────────────────────────────────────────────────────────────

def ensure_dirs():
    """Create all required data directories."""
    for d in [RAW_DIR, FAO_DIR, NOAA_DIR, MODIS_DIR, GADM_DIR]:
        os.makedirs(d, exist_ok=True)
    print("✓ Data directories created.")


def download_file(url, dest_path, description="file"):
    """Download a file with progress reporting."""
    print(f"  Downloading {description}...")
    print(f"  URL: {url}")
    try:
        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        downloaded = 0
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
                downloaded += len(chunk)
                if total > 0:
                    pct = (downloaded / total) * 100
                    print(f"\r  Progress: {downloaded / 1e6:.1f} MB / {total / 1e6:.1f} MB ({pct:.0f}%)", end="", flush=True)
        print(f"\n  ✓ Saved to {dest_path}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"\n  ✗ Download failed: {e}")
        return False


# ──────────────────────────────────────────────────────────────────────
# 1. FAOSTAT Crop Production
# ──────────────────────────────────────────────────────────────────────

def download_fao(years):
    """Download and filter FAOSTAT crop production data."""
    print("\n" + "=" * 70)
    print("  [1/4] FAOSTAT — Crop Production Statistics")
    print("=" * 70)

    output_csv = os.path.join(FAO_DIR, "fao_crop_production.csv")
    if os.path.exists(output_csv):
        print(f"  ⏩ Already exists: {output_csv}")
        return True

    bulk_url = "https://bulks-faostat.fao.org/production/Production_Crops_Livestock_E_All_Data_(Normalized).zip"
    print(f"  Source: FAOSTAT Bulk Download (Crops & Livestock)")

    try:
        print("  Downloading bulk ZIP (~200 MB, may take a few minutes)...")
        resp = requests.get(bulk_url, stream=True, timeout=300)
        resp.raise_for_status()

        # Read zip in memory and extract
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        csv_names = [n for n in z.namelist() if n.endswith(".csv")]
        if not csv_names:
            print("  ✗ No CSV found inside ZIP.")
            return False

        print(f"  Extracting: {csv_names[0]}")
        df = pd.read_csv(z.open(csv_names[0]), encoding="ISO-8859-1", low_memory=False)
        print(f"  Raw rows: {len(df):,}")

        # Filter by target countries
        country_names = [c["name"] for c in TARGET_COUNTRIES.values()]
        # Also try partial matching for China variants
        mask_country = df["Area"].isin(country_names) | df["Area"].str.contains("China", na=False)
        df = df[mask_country]

        # Filter by target crops
        crop_names = list(TARGET_CROPS.keys())
        # Use flexible matching for crop names
        crop_patterns = "|".join([c.split("(")[0].strip().lower() for c in crop_names])
        mask_crop = df["Item"].str.lower().str.contains(crop_patterns, na=False)
        df = df[mask_crop]

        # Filter by years
        df = df[df["Year"].isin(years)]

        # Filter by elements we care about: Yield, Area harvested, Production
        target_elements = ["Yield", "Area harvested", "Production"]
        mask_elem = df["Element"].str.lower().str.contains("|".join([e.lower() for e in target_elements]), na=False)
        df = df[mask_elem]

        print(f"  Filtered rows: {len(df):,}")
        df.to_csv(output_csv, index=False)
        print(f"  ✓ Saved to {output_csv}")
        return True

    except Exception as e:
        print(f"  ✗ FAO download failed: {e}")
        return False


# ──────────────────────────────────────────────────────────────────────
# 2. NOAA GSOD Weather Observations
# ──────────────────────────────────────────────────────────────────────

def download_noaa(years):
    """Download NOAA GSOD daily weather from AWS S3 public bucket."""
    print("\n" + "=" * 70)
    print("  [2/4] NOAA GSOD — Daily Weather Observations")
    print("=" * 70)

    # Step 1: Download station inventory
    inventory_path = os.path.join(NOAA_DIR, "isd-history.csv")
    if not os.path.exists(inventory_path):
        inv_url = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"
        if not download_file(inv_url, inventory_path, "station inventory"):
            return False

    # Read inventory and find stations in target countries
    print("  Reading station inventory...")
    inv_df = pd.read_csv(inventory_path, dtype=str)
    # Clean column names (they may have extra spaces)
    inv_df.columns = inv_df.columns.str.strip()

    # Filter for target country FIPS codes
    target_fips = [c["noaa_fips"] for c in TARGET_COUNTRIES.values()]
    inv_df = inv_df[inv_df["CTRY"].isin(target_fips)]

    # Filter for stations active during our target years
    inv_df["BEGIN"] = pd.to_numeric(inv_df["BEGIN"], errors="coerce")
    inv_df["END"] = pd.to_numeric(inv_df["END"], errors="coerce")
    min_year = min(years)
    max_year = max(years)
    max_history_date = inv_df["END"].max()
    inv_df = inv_df[
        (inv_df["BEGIN"] <= min_year * 10000 + 101) &
        (inv_df["END"] >= min(max_year * 10000 + 1231, int(max_history_date)))
    ]
    # Convert lat/lon to numeric
    inv_df["LAT"] = pd.to_numeric(inv_df["LAT"], errors="coerce")
    inv_df["LON"] = pd.to_numeric(inv_df["LON"], errors="coerce")
    inv_df = inv_df.dropna(subset=["LAT", "LON"])

    # Select top N stations per country (best coverage, most central)
    selected_stations = []
    for iso3, info in TARGET_COUNTRIES.items():
        fips = info["noaa_fips"]
        country_stations = inv_df[inv_df["CTRY"] == fips].copy()

        if len(country_stations) == 0:
            print(f"  ⚠ No stations found for {info['name']}")
            continue

        # Take stations with valid USAF/WBAN IDs, limited to MAX_STATIONS_PER_COUNTRY
        # Prefer stations with more data (longer BEGIN-END range)
        country_stations["duration"] = country_stations["END"] - country_stations["BEGIN"]
        country_stations = country_stations.nlargest(MAX_STATIONS_PER_COUNTRY, "duration")

        selected_stations.append(country_stations)
        print(f"  Selected {len(country_stations)} stations for {info['name']}")

    if not selected_stations:
        print("  ✗ No stations selected.")
        return False

    all_stations = pd.concat(selected_stations, ignore_index=True)
    print(f"  Total stations to download: {len(all_stations)}")

    # Save station metadata
    all_stations.to_csv(os.path.join(NOAA_DIR, "selected_stations.csv"), index=False)

    # Step 2: Download weather data from AWS S3
    s3_base = "https://noaa-gsod-pds.s3.amazonaws.com"
    total_files = len(all_stations) * len(years)
    
    # Prepare download tasks
    download_tasks = []
    for year in years:
        year_dir = os.path.join(NOAA_DIR, str(year))
        os.makedirs(year_dir, exist_ok=True)

        for _, station in all_stations.iterrows():
            usaf = str(station["USAF"]).strip()
            wban = str(station["WBAN"]).strip()
            station_id = f"{usaf}{wban}"
            dest_file = os.path.join(year_dir, f"{station_id}.csv")
            url = f"{s3_base}/{year}/{station_id}.csv"
            download_tasks.append((url, dest_file))

    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    downloaded = 0
    failed = 0
    
    def download_one(task):
        url, dest_file = task
        if os.path.exists(dest_file):
            return True
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                with open(dest_file, "wb") as f:
                    f.write(r.content)
                return True
        except Exception:
            pass
        return False

    print(f"  Downloading weather data concurrently with 20 threads...")
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(download_one, task): task for task in download_tasks}
        for future in as_completed(futures):
            ok = future.result()
            if ok:
                downloaded += 1
            else:
                failed += 1
            total_done = downloaded + failed
            if total_done % 20 == 0:
                print(f"\r  Progress: {total_done}/{total_files} files (✓{downloaded} ✗{failed})", end="", flush=True)

    print(f"\n  ✓ Downloaded {downloaded} station-year files ({failed} failed)")

    # Step 3: Consolidate into a single CSV for pipeline use
    print("  Consolidating NOAA data into a single processed CSV...")
    consolidate_noaa_data(years, all_stations)
    return True


def consolidate_noaa_data(years, stations_df):
    """Merge individual GSOD files into one processed CSV with consistent schema."""
    all_records = []
    station_country_map = {}

    for _, station in stations_df.iterrows():
        usaf = str(station["USAF"]).strip()
        wban = str(station["WBAN"]).strip()
        station_id = f"{usaf}{wban}"
        station_country_map[station_id] = station["CTRY"]

    for year in years:
        year_dir = os.path.join(NOAA_DIR, str(year))
        if not os.path.isdir(year_dir):
            continue

        for csv_file in glob.glob(os.path.join(year_dir, "*.csv")):
            try:
                df = pd.read_csv(csv_file, dtype=str)
                if df.empty:
                    continue

                station_id = os.path.basename(csv_file).replace(".csv", "")
                ctry = station_country_map.get(station_id, "??")

                # Parse relevant columns — GSOD uses Fahrenheit and inches
                for _, row in df.iterrows():
                    try:
                        temp_f = float(row.get("TEMP", "9999.9"))
                        prcp_in = str(row.get("PRCP", "99.99"))
                        # Remove trailing flags from PRCP (e.g., "0.00G")
                        prcp_in = "".join(c for c in prcp_in if c.isdigit() or c == ".")
                        prcp_val = float(prcp_in) if prcp_in else 0.0

                        # Skip missing values (9999.9 = missing in GSOD)
                        if temp_f > 9000:
                            continue

                        # Convert Fahrenheit → Celsius, inches → mm
                        temp_c = round((temp_f - 32) * 5 / 9, 2)
                        precip_mm = round(prcp_val * 25.4, 2) if prcp_val < 90 else 0.0

                        all_records.append({
                            "station_id": station_id,
                            "country": ctry,
                            "date": row.get("DATE", ""),
                            "lat": row.get("LATITUDE", ""),
                            "lon": row.get("LONGITUDE", ""),
                            "temp_c": temp_c,
                            "precip_mm": precip_mm,
                        })
                    except (ValueError, TypeError):
                        continue
            except Exception:
                continue

    if all_records:
        out_df = pd.DataFrame(all_records)
        out_path = os.path.join(NOAA_DIR, "noaa_weather_consolidated.csv")
        out_df.to_csv(out_path, index=False)
        print(f"  ✓ Consolidated {len(out_df):,} weather records → {out_path}")
    else:
        print("  ⚠ No weather records consolidated.")


# ──────────────────────────────────────────────────────────────────────
# 3. NASA MODIS NDVI GeoTIFF
# ──────────────────────────────────────────────────────────────────────

def download_modis(years):
    """Provide MODIS download instructions and generate AppEEARS request."""
    print("\n" + "=" * 70)
    print("  [3/4] NASA MODIS — Satellite NDVI (MOD13Q1) GeoTIFF")
    print("=" * 70)

    existing = glob.glob(os.path.join(MODIS_DIR, "**/*.tif"), recursive=True)
    if existing:
        print(f"  ⏩ Found {len(existing)} existing GeoTIFF files in {MODIS_DIR}")
        return True

    print("""
  ┌─────────────────────────────────────────────────────────────────┐
  │  MODIS data requires a free NASA Earthdata Login.              │
  │  Please follow the instructions below to download GeoTIFF      │
  │  tiles manually.                                               │
  └─────────────────────────────────────────────────────────────────┘

  OPTION A: AppEEARS (Recommended — Automated Subsetting)
  ────────────────────────────────────────────────────────
  1. Create a free account at: https://urs.earthdata.nasa.gov/
  2. Go to: https://appeears.earthdatacloud.nasa.gov/
  3. Click "Extract" → "Area" → draw/upload regions or use coordinates
  4. Product: MOD13Q1.061 (MODIS/Terra Vegetation Indices 16-Day L3 250m)
  5. Layer: _250m_16_days_NDVI
  6. Date range: {start} to {end}
  7. Output format: GeoTIFF
  8. Submit request → download results when ready
  9. Place all .tif files in: {modis_dir}/

  OPTION B: Earthdata Search (Manual Tile Download)
  ──────────────────────────────────────────────────
  1. Go to: https://search.earthdata.nasa.gov/
  2. Search for: MOD13Q1
  3. Filter by date: {start} to {end}
  4. Filter by tile (granule): see tiles below
  5. Download HDF files → convert to GeoTIFF with gdal_translate

  Required MODIS Tiles:
""".format(
        start=f"{min(years)}-01-01",
        end=f"{max(years)}-12-31",
        modis_dir=os.path.abspath(MODIS_DIR),
    ))

    for iso3, tile in MODIS_TILES.items():
        country_name = TARGET_COUNTRIES[iso3]["name"]
        print(f"    {tile}  →  {country_name}")

    print(f"\n  Place downloaded .tif files in:\n    {os.path.abspath(MODIS_DIR)}/")
    print()

    # Generate AppEEARS task JSON for programmatic use
    appeears_task = {
        "task_type": "area",
        "task_name": "ClimateSmartAg_MODIS_NDVI",
        "params": {
            "dates": [
                {"startDate": f"01-01-{min(years)}", "endDate": f"12-31-{max(years)}"}
            ],
            "layers": [
                {"product": "MOD13Q1.061", "layer": "_250m_16_days_NDVI"}
            ],
            "output": {
                "format": {"type": "geotiff"},
                "projection": "geographic"
            },
            "geo": {
                "type": "FeatureCollection",
                "features": []  # User fills with their ROI
            },
        },
    }
    task_path = os.path.join(MODIS_DIR, "appeears_task_template.json")
    with open(task_path, "w") as f:
        json.dump(appeears_task, f, indent=2)
    print(f"  ✓ AppEEARS task template saved to: {task_path}")
    print("    (Edit the 'geo' field with your region of interest, then submit via API)")
    return True


# ──────────────────────────────────────────────────────────────────────
# 4. GADM Administrative Boundaries
# ──────────────────────────────────────────────────────────────────────

def download_gadm():
    """Download GADM shapefiles for target countries."""
    print("\n" + "=" * 70)
    print("  [4/4] GADM — Administrative Boundaries (Shapefiles)")
    print("=" * 70)

    gadm_base_url = "https://geodata.ucdavis.edu/gadm/gadm4.1/shp"
    success_count = 0

    for iso3, info in TARGET_COUNTRIES.items():
        country_dir = os.path.join(GADM_DIR, iso3)
        os.makedirs(country_dir, exist_ok=True)

        # Check if already downloaded
        shp_files = glob.glob(os.path.join(country_dir, "*.shp"))
        if shp_files:
            print(f"  ⏩ {info['name']} — already downloaded ({len(shp_files)} .shp files)")
            success_count += 1
            continue

        # Download admin level 1 shapefile
        zip_name = f"gadm41_{iso3}_shp.zip"
        zip_url = f"{gadm_base_url}/{zip_name}"
        zip_path = os.path.join(country_dir, zip_name)

        if download_file(zip_url, zip_path, f"{info['name']} shapefile"):
            # Extract
            try:
                with zipfile.ZipFile(zip_path, "r") as z:
                    z.extractall(country_dir)
                print(f"  ✓ Extracted {info['name']} shapefiles to {country_dir}")
                os.remove(zip_path)  # Clean up zip
                success_count += 1
            except zipfile.BadZipFile:
                print(f"  ✗ Bad ZIP file for {info['name']}")

    print(f"\n  ✓ Downloaded shapefiles for {success_count}/{len(TARGET_COUNTRIES)} countries")
    return success_count > 0


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Download real-world datasets for Climate-Smart Agriculture pipeline"
    )
    parser.add_argument(
        "--source",
        choices=["all", "fao", "noaa", "modis", "gadm"],
        default="all",
        help="Which data source to download (default: all)",
    )
    parser.add_argument(
        "--years",
        nargs=2,
        type=int,
        default=[2015, 2019],
        metavar=("START", "END"),
        help="Year range to download (default: 2015 2019)",
    )
    args = parser.parse_args()

    years = list(range(args.years[0], args.years[1] + 1))
    print("\n╔════════════════════════════════════════════════════════════════╗")
    print("║  CLIMATE-SMART AGRICULTURE — Data Download Tool              ║")
    print("╠════════════════════════════════════════════════════════════════╣")
    print(f"║  Source:  {args.source:<51}║")
    print(f"║  Years:   {args.years[0]}–{args.years[1]:<47}║")
    print(f"║  Countries: US (5 agricultural states)                       ║")
    print(f"║  Crops:   Maize, Wheat, Soybeans                             ║")
    print("╚════════════════════════════════════════════════════════════════╝")

    ensure_dirs()

    results = {}
    if args.source in ("all", "fao"):
        results["fao"] = download_fao(years)
    if args.source in ("all", "noaa"):
        results["noaa"] = download_noaa(years)
    if args.source in ("all", "modis"):
        results["modis"] = download_modis(years)
    if args.source in ("all", "gadm"):
        results["gadm"] = download_gadm()

    # Summary
    print("\n" + "=" * 70)
    print("  Download Summary")
    print("=" * 70)
    for src, ok in results.items():
        status = "✓ Success" if ok else "✗ Failed"
        print(f"  {src.upper():<10} {status}")
    print("=" * 70)

    if all(results.values()):
        print("\n  All downloads completed! Next steps:")
        print("  1. Place MODIS GeoTIFF files in data/raw/modis/")
        print("  2. Run: bash run_all.sh")
    print()


if __name__ == "__main__":
    main()
