#!/usr/bin/env python3
"""
Climate-Smart Agriculture — Simulated Apache Flume Ingestion Agent

Simulates the behavior of an Apache Flume agent with:
  - Source: watches a local directory for new weather CSV files
  - Channel: in-memory buffer with configurable capacity
  - Sink: writes batched records to HDFS

This demonstrates the Flume ingestion pattern used in big data pipelines
for streaming semi-structured data into HDFS.

Usage:
  # Producer mode: generate weather update files at a configurable rate
  python flume_agent.py --mode producer --rate 5 --duration 30

  # Agent mode: watch directory and ingest files into HDFS
  python flume_agent.py --mode agent --watch-dir data/streaming_inbox --hdfs-dir /data/streaming/weather_incoming

  # Both: run producer + agent together for a demo
  python flume_agent.py --mode demo --duration 60
"""

import os
import sys
import time
import csv
import glob
import shutil
import random
import argparse
import threading
import json
import numpy as np
from datetime import datetime, timedelta
from collections import deque


# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────

WATCH_DIR = os.getenv("FLUME_WATCH_DIR", "data/streaming_inbox")
HDFS_TARGET = os.getenv("FLUME_HDFS_DIR", "/data/streaming/weather_incoming")
PROCESSED_DIR = os.getenv("FLUME_PROCESSED_DIR", "data/streaming_processed")
CHANNEL_CAPACITY = 10000   # Max records in memory channel
BATCH_SIZE = 500           # Records per HDFS write
FLUSH_INTERVAL = 5         # Seconds between flushes


# ──────────────────────────────────────────────────────────────────────
# Flume Channel (in-memory buffer)
# ──────────────────────────────────────────────────────────────────────

class MemoryChannel:
    """In-memory channel simulating Flume's memory channel."""

    def __init__(self, capacity=CHANNEL_CAPACITY):
        self.buffer = deque(maxlen=capacity)
        self.lock = threading.Lock()
        self.total_put = 0
        self.total_take = 0

    def put(self, records):
        with self.lock:
            for r in records:
                self.buffer.append(r)
                self.total_put += 1

    def take(self, batch_size=BATCH_SIZE):
        with self.lock:
            batch = []
            while self.buffer and len(batch) < batch_size:
                batch.append(self.buffer.popleft())
                self.total_take += 1
            return batch

    def size(self):
        return len(self.buffer)

    def stats(self):
        return {
            "channel_size": len(self.buffer),
            "total_put": self.total_put,
            "total_take": self.total_take,
        }


# ──────────────────────────────────────────────────────────────────────
# Flume Source: Directory Watcher
# ──────────────────────────────────────────────────────────────────────

class SpoolDirSource:
    """Watches a directory for new CSV files (like Flume's Spooling Dir Source)."""

    def __init__(self, watch_dir, channel):
        self.watch_dir = watch_dir
        self.channel = channel
        self.processed_files = set()
        self.files_ingested = 0
        self.records_ingested = 0

    def poll(self):
        """Check for new files and ingest them into the channel."""
        os.makedirs(self.watch_dir, exist_ok=True)
        csv_files = sorted(glob.glob(os.path.join(self.watch_dir, "*.csv")))

        new_files = [f for f in csv_files if f not in self.processed_files]
        if not new_files:
            return 0

        total_records = 0
        for filepath in new_files:
            try:
                records = []
                with open(filepath, "r") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        records.append(row)

                self.channel.put(records)
                total_records += len(records)
                self.processed_files.add(filepath)
                self.files_ingested += 1
                self.records_ingested += len(records)

                # Move to processed directory (Flume behavior)
                os.makedirs(PROCESSED_DIR, exist_ok=True)
                dest = os.path.join(PROCESSED_DIR, os.path.basename(filepath))
                shutil.move(filepath, dest)

                timestamp = datetime.now().strftime("%H:%M:%S")
                print(f"  [{timestamp}] SOURCE: Ingested {len(records)} records from {os.path.basename(filepath)}")

            except Exception as e:
                print(f"  ⚠ Error reading {filepath}: {e}")

        return total_records

    def stats(self):
        return {
            "files_ingested": self.files_ingested,
            "records_ingested": self.records_ingested,
        }


# ──────────────────────────────────────────────────────────────────────
# Flume Sink: HDFS Writer
# ──────────────────────────────────────────────────────────────────────

class HDFSSink:
    """Writes batched records to HDFS (via Docker exec to namenode)."""

    def __init__(self, channel, hdfs_dir):
        self.channel = channel
        self.hdfs_dir = hdfs_dir
        self.files_written = 0
        self.records_written = 0
        self._ensure_hdfs_dir()

    def _ensure_hdfs_dir(self):
        """Create HDFS directory if it doesn't exist."""
        import requests
        try:
            requests.put(f"http://namenode:9870/webhdfs/v1{self.hdfs_dir}?op=MKDIRS")
        except Exception:
            pass

    def flush(self):
        """Take a batch from the channel and write to HDFS."""
        batch = self.channel.take(BATCH_SIZE)
        if not batch:
            return 0

        # Write batch to a temp CSV file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        tmp_file = f"/tmp/flume_batch_{timestamp}.csv"

        try:
            with open(tmp_file, "w", newline="") as f:
                if batch:
                    writer = csv.DictWriter(f, fieldnames=batch[0].keys())
                    writer.writeheader()
                    writer.writerows(batch)

            # Upload to HDFS via WebHDFS
            import requests
            hdfs_path = f"{self.hdfs_dir}/weather_batch_{timestamp}.csv"
            hdfs_url = f"http://namenode:9870/webhdfs/v1{hdfs_path}?op=CREATE&overwrite=true"
            r = requests.put(hdfs_url, allow_redirects=False)
            if r.status_code == 307:
                redirect_url = r.headers['Location']
                with open(tmp_file, 'rb') as f:
                    r2 = requests.put(redirect_url, data=f)
                if r2.status_code not in (200, 201):
                    raise Exception(f"Failed to write to HDFS datanode: {r2.status_code} {r2.text}")
            else:
                raise Exception(f"Failed to initiate HDFS write: {r.status_code} {r.text}")
            os.remove(tmp_file)

            self.files_written += 1
            self.records_written += len(batch)

            ts = datetime.now().strftime("%H:%M:%S")
            print(f"  [{ts}] SINK: Wrote {len(batch)} records → {hdfs_path}")
            return len(batch)

        except Exception as e:
            print(f"  ⚠ HDFS sink error: {e}")
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
            return 0

    def stats(self):
        return {
            "files_written": self.files_written,
            "records_written": self.records_written,
        }


# ──────────────────────────────────────────────────────────────────────
# Weather Data Producer (Live API)
# ──────────────────────────────────────────────────────────────────────

# Mapping of the 5 agricultural regions in the USA (county_ids 1 to 5) to approximate (lat, lon) coordinates
REGION_COORDS = {
    # USA
    1: (42.0, -93.0),   # Iowa
    2: (40.0, -89.0),   # Illinois
    3: (39.8, -86.1),   # Indiana
    4: (41.5, -99.8),   # Nebraska
    5: (38.5, -98.0),   # Kansas
}

def fetch_live_weather_files(output_dir, rate=60, duration=3600):
    """Fetch real-time weather observations from Open-Meteo API."""
    import requests
    print(f"\n  PRODUCER: Fetching live weather data every {rate}s for {duration}s")
    print(f"  Output directory: {output_dir}")
    os.makedirs(output_dir, exist_ok=True)

    start_time = time.time()
    file_count = 0
    
    # Pick regions per batch to avoid overly massive single API calls and simulate distributed streaming
    county_ids = list(REGION_COORDS.keys())

    while time.time() - start_time < duration:
        batch_counties = random.sample(county_ids, min(5, len(county_ids)))
        lats = [REGION_COORDS[c][0] for c in batch_counties]
        lons = [REGION_COORDS[c][1] for c in batch_counties]
        
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lats,
            "longitude": lons,
            "current": "temperature_2m,precipitation"
        }
        
        records = []
        obs_date = datetime.now()
        
        try:
            resp = requests.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            
            # The API returns a list when multiple coordinates are passed
            if not isinstance(data, list):
                data = [data]
                
            for idx, c_data in enumerate(data):
                county_id = batch_counties[idx]
                current = c_data.get("current", {})
                temp = current.get("temperature_2m", 20.0)
                precip = current.get("precipitation", 0.0)
                
                records.append({
                    "county_id": county_id,
                    "date": obs_date.strftime("%Y-%m-%d"),
                    "temp_c": round(temp, 2) if temp is not None else 20.0,
                    "precip_mm": round(precip, 2) if precip is not None else 0.0,
                })
                
            # Write CSV file
            filename = f"weather_update_{obs_date.strftime('%Y%m%d_%H%M%S')}.csv"
            filepath = os.path.join(output_dir, filename)
            with open(filepath, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["county_id", "date", "temp_c", "precip_mm"])
                writer.writeheader()
                writer.writerows(records)

            file_count += 1
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"  [{ts}] PRODUCER: Fetched live data for {filename} ({len(records)} records)")
            
        except Exception as e:
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] PRODUCER Error: Failed to fetch live data: {e}")

        time.sleep(rate)

    print(f"  PRODUCER: Finished. Generated {file_count} files.")
    return file_count


# ──────────────────────────────────────────────────────────────────────
# Flume Agent Runner
# ──────────────────────────────────────────────────────────────────────

def run_agent(watch_dir, hdfs_dir, duration=60):
    """Run the simulated Flume agent for a specified duration."""
    print(f"\n{'='*60}")
    print(f"  FLUME AGENT — Starting")
    print(f"  Watch dir:  {watch_dir}")
    print(f"  HDFS target: {hdfs_dir}")
    print(f"  Duration:    {duration}s")
    print(f"  Batch size:  {BATCH_SIZE}")
    print(f"  Flush every: {FLUSH_INTERVAL}s")
    print(f"{'='*60}\n")

    channel = MemoryChannel()
    source = SpoolDirSource(watch_dir, channel)
    sink = HDFSSink(channel, hdfs_dir)

    start_time = time.time()
    while time.time() - start_time < duration:
        # Source: poll for new files
        source.poll()

        # Sink: flush buffered records to HDFS
        if channel.size() > 0:
            sink.flush()

        time.sleep(1)

    # Final flush
    while channel.size() > 0:
        sink.flush()

    # Print summary
    print(f"\n{'='*60}")
    print("  FLUME AGENT — Summary")
    print(f"{'='*60}")
    s_stats = source.stats()
    c_stats = channel.stats()
    k_stats = sink.stats()
    print(f"  Source:   {s_stats['files_ingested']} files, {s_stats['records_ingested']} records ingested")
    print(f"  Channel:  {c_stats['total_put']} put, {c_stats['total_take']} taken, {c_stats['channel_size']} remaining")
    print(f"  Sink:     {k_stats['files_written']} batches written, {k_stats['records_written']} records to HDFS")
    print(f"{'='*60}\n")

    return k_stats


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Simulated Apache Flume Ingestion Agent")
    parser.add_argument("--mode", choices=["producer", "agent", "demo"], default="demo",
                        help="Operating mode: producer (generate files), agent (ingest to HDFS), demo (both)")
    parser.add_argument("--watch-dir", default=WATCH_DIR, help="Directory to watch for new files")
    parser.add_argument("--hdfs-dir", default=HDFS_TARGET, help="HDFS target directory")
    parser.add_argument("--rate", type=int, default=60, help="Seconds between produced files (producer mode)")
    parser.add_argument("--duration", type=int, default=300, help="Duration in seconds")
    args = parser.parse_args()

    print("\n╔════════════════════════════════════════════════════════════════╗")
    print("║  CLIMATE-SMART AGRICULTURE — Flume Ingestion Agent           ║")
    print("╚════════════════════════════════════════════════════════════════╝")

    if args.mode == "producer":
        fetch_live_weather_files(args.watch_dir, rate=args.rate, duration=args.duration)

    elif args.mode == "agent":
        run_agent(args.watch_dir, args.hdfs_dir, duration=args.duration)

    elif args.mode == "demo":
        # Run producer and agent concurrently
        producer_thread = threading.Thread(
            target=fetch_live_weather_files,
            args=(args.watch_dir, args.rate, args.duration),
            daemon=True,
        )
        producer_thread.start()

        # Small delay to let first file arrive
        time.sleep(2)
        run_agent(args.watch_dir, args.hdfs_dir, duration=args.duration + 5)
        producer_thread.join(timeout=5)


if __name__ == "__main__":
    main()
