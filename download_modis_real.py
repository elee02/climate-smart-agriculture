#!/usr/bin/env python3
"""
Climate-Smart Agriculture — Automated NASA MODIS Real Data Downloader

Downloads real MOD13Q1.061 NDVI HDF files from NASA LP DAAC using the earthaccess
library, extracts the NDVI layer using rasterio, and saves them as GeoTIFF
files in data/raw/modis/.

Prerequisites:
  - Free NASA Earthdata Login: https://urs.earthdata.nasa.gov/
  - Python packages: earthaccess, rasterio, numpy, pandas

Usage:
  python download_modis_real.py --username YOUR_USERNAME --password YOUR_PASSWORD --year 2024
"""

import os
import sys
import argparse
import earthaccess
import rasterio
import cmr

# Fix python-cmr parameter encoding bug causing NASA CMR 500 errors (bracketed keys like temporal[])
original_build_url = cmr.queries.Query._build_url

def patched_build_url(self):
    url = original_build_url(self)
    return url.replace("[]=", "=").replace("%5B%5D=", "=")

cmr.queries.Query._build_url = patched_build_url

# Target tiles matching target countries
TARGET_TILES = [
    "h10v04", "h10v05", "h11v04", "h11v05", "h12v10", "h13v10", "h13v11",
    "h21v08", "h21v09", "h24v05", "h24v06", "h25v06", "h25v07", "h26v04",
    "h27v05", "h28v05"
]
MODIS_DIR = "data/raw/modis"


def extract_ndvi_to_tiff(hdf_path, tiff_path):
    """Open HDF file with pyhdf, read the NDVI subdataset, and write as GeoTIFF."""
    try:
        from pyhdf.SD import SD, SDC
        from rasterio.transform import Affine
        import re
        import os

        # Read the NDVI data array
        hdf = SD(hdf_path, SDC.READ)
        sds = hdf.select('250m 16 days NDVI')
        data = sds.get()
        nodata = -3000

        # Calculate transform from the tile name in the filename
        filename = os.path.basename(hdf_path)
        match = re.search(r'\.h(\d{2})v(\d{2})\.', filename)
        if not match:
            print(f"    ✗ Could not parse tile from filename: {filename}")
            return False
        
        h = int(match.group(1))
        v = int(match.group(2))
        
        # MODIS Sinusoidal constants
        TILE_SIZE = 1111950.5196666666
        X_MIN = -20015109.354
        Y_MAX = 10007554.677
        
        x_min = X_MIN + h * TILE_SIZE
        y_max = Y_MAX - v * TILE_SIZE
        
        pixel_size = TILE_SIZE / 4800
        transform = Affine(pixel_size, 0.0, x_min, 0.0, -pixel_size, y_max)
        crs = "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +R=6371007.181 +units=m +no_defs"
        
        profile = {
            'driver': 'GTiff',
            'height': data.shape[0],
            'width': data.shape[1],
            'count': 1,
            'dtype': str(data.dtype),
            'crs': crs,
            'transform': transform,
            'nodata': nodata,
            'compress': 'lzw'
        }
        
        with rasterio.open(tiff_path, "w", **profile) as dst:
            dst.write(data, 1)
            
        return True
    except Exception as e:
        print(f"    ✗ Failed to convert HDF to GeoTIFF: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Automated NASA MODIS Downloader")
    parser.add_argument("--username", help="NASA Earthdata username")
    parser.add_argument("--password", help="NASA Earthdata password")
    parser.add_argument("--token", help="NASA Earthdata token (Generate Token on EarthData page)")
    parser.add_argument("--year", type=int, help="Single target year to download")
    parser.add_argument("--years", nargs=2, type=int, default=[2015, 2026], metavar=("START", "END"), help="Target years range to download (default: 2015 2026)")
    args = parser.parse_args()

    os.makedirs(MODIS_DIR, exist_ok=True)
    
    print("\nLogging into NASA Earthdata...")
    try:
        token = args.token or os.environ.get("EARTHDATA_TOKEN")
        username = args.username or os.environ.get("EARTHDATA_USERNAME")
        password = args.password or os.environ.get("EARTHDATA_PASSWORD")

        if token:
            cleaned_token = token.replace("\n", "").replace("\r", "").replace(" ", "").replace("\\n", "")
            os.environ["EARTHDATA_TOKEN"] = cleaned_token
            print("Authenticating with NASA Earthdata Token (bypassing EDL credentials)...")
            auth = earthaccess.Auth()
            auth.username = "elee02"
            auth.password = ""
            auth.token = {"access_token": cleaned_token}
            auth.authenticated = True
            
            earthaccess.__auth__ = auth
            earthaccess.__store__ = earthaccess.store.Store(auth)
        elif username and password:
            os.environ["EARTHDATA_USERNAME"] = username
            os.environ["EARTHDATA_PASSWORD"] = password
            print("Authenticating with Username and Password...")
            auth = earthaccess.login(strategy="environment")
        else:
            print("Attempting authentication via netrc or interactive login...")
            auth = earthaccess.login(strategy="interactive")

        if not auth.authenticated:
            print("Authentication failed. Please verify credentials or token.")
            print("\n💡 TIP: If you receive a 404 error during download, ensure that you have authorized")
            print("the 'LPDAAC_ECS' and 'NASA GESDISC DATA POOL' applications in your NASA Earthdata Profile:")
            print("1. Log in to https://urs.earthdata.nasa.gov/")
            print("2. Navigate to 'Applications' -> 'Authorized Apps'")
            print("3. If LPDAAC_ECS is not listed, click 'Approve More Applications' and approve it.")
            sys.exit(1)
    except Exception as e:
        print(f"Login failed: {e}")
        print("\n💡 TIP: Ensure that you have authorized the 'LPDAAC_ECS' application in your NASA Earthdata Profile.")
        sys.exit(1)

    if args.year:
        years = [args.year]
    else:
        years = list(range(args.years[0], args.years[1] + 1))

    total_success_count = 0
    total_granules_processed = 0

    for year in years:
        print(f"\nSearching for MOD13Q1.061 granules for year {year}...")
        try:
            results = earthaccess.search_data(
                short_name="MOD13Q1",
                version="061",
                temporal=(f"{year}-01-01", f"{year}-12-31")
            )
        except Exception as e:
            print(f"Search failed for year {year}: {e}")
            continue

        print(f"Found {len(results)} total granules. Filtering by target tiles: {TARGET_TILES}...")
        
        # Filter granules by target tiles
        filtered_results = []
        for granule in results:
            filename = granule["meta"]["native-id"] + ".hdf"
            if any(tile in filename for tile in TARGET_TILES):
                filtered_results.append(granule)

        print(f"Found {len(filtered_results)} matching granules for target tiles in year {year}.")
        if not filtered_results:
            print(f"No matching granules found for year {year}.")
            continue

        total_granules_processed += len(filtered_results)

        # Download and process each granule
        for idx, granule in enumerate(filtered_results):
            filename = granule["meta"]["native-id"] + ".hdf"
            print(f"\n[{idx+1}/{len(filtered_results)}] Processing {filename}...")
            
            # Local GeoTIFF target path
            local_tiff = os.path.join(MODIS_DIR, filename.replace(".hdf", ".tif"))

            if os.path.exists(local_tiff):
                print(f"    ⏩ GeoTIFF already exists, skipping.")
                total_success_count += 1
                continue

            try:
                # Download single HDF granule
                paths = earthaccess.download([granule], local_path=MODIS_DIR)
                if not paths:
                    print(f"    ✗ Download failed for {filename}")
                    print("\n    💡 TIP: If the download fails with a 404 or unauthorized error, ensure that you have")
                    print("       authorized the 'LPDAAC_ECS' and 'NASA GESDISC DATA POOL' applications in your NASA Earthdata Profile:")
                    print("       1. Log in to https://urs.earthdata.nasa.gov/")
                    print("       2. Navigate to 'Applications' -> 'Authorized Apps'")
                    print("       3. If LPDAAC_ECS is not listed, click 'Approve More Applications' and approve it.")
                    continue
                
                local_hdf = next((str(p) for p in paths if str(p).endswith('.hdf')), None)
                if not local_hdf:
                    print(f"    ✗ No HDF file downloaded for {filename}")
                    continue
                
                # Convert to GeoTIFF
                print(f"    Converting to GeoTIFF...")
                success = extract_ndvi_to_tiff(local_hdf, local_tiff)
                
                # Clean up the downloaded HDF
                if os.path.exists(local_hdf):
                    os.remove(local_hdf)

                if success:
                    total_success_count += 1
                    print(f"    ✓ Successfully saved: {os.path.basename(local_tiff)}")
            except Exception as e:
                print(f"    ✗ Error processing {filename}: {e}")

    print(f"\n✓ Completed. Successfully processed {total_success_count}/{total_granules_processed} tiles.")
    print(f"All GeoTIFF files are located in: {os.path.abspath(MODIS_DIR)}/")


if __name__ == "__main__":
    main()
