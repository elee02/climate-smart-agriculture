#!/bin/bash
set -e

# Function to check if a process with a given name is running in /proc
check_process() {
    python3 -c "
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

# Wait for HBase Thrift server
echo "Waiting for HBase Thrift server (hbase:9090)..."
until python3 -c "import socket; s = socket.socket(); s.settimeout(1); s.connect(('hbase', 9090))" 2>/dev/null; do
    sleep 2
done
echo "HBase Thrift server is ready!"

# Wait for HDFS NameNode
echo "Waiting for HDFS NameNode (namenode:9870)..."
until python3 -c "import socket; s = socket.socket(); s.settimeout(1); s.connect(('namenode', 9870))" 2>/dev/null; do
    sleep 2
done
echo "HDFS NameNode is ready!"

# Ensure HDFS directories for streaming exist
curl -s -X PUT "http://namenode:9870/webhdfs/v1/data/streaming/weather_incoming?op=MKDIRS" >/dev/null || true
curl -s -X PUT "http://namenode:9870/webhdfs/v1/data/streaming/checkpoints/weather_stream?op=MKDIRS" >/dev/null || true

# Start background Flume agent simulation if not already running
if ! check_process "flume_agent.py"; then
    echo "Starting background Flume agent..."
    python3 -u flume_agent.py --mode demo --rate 1800 --duration 999999 > /app/data/flume_agent.log 2>&1 &
else
    echo "Flume agent is already running."
fi

# Start background Spark structured streaming if not already running
if ! check_process "spark_streaming.py"; then
    echo "Starting background Spark Structured Streaming..."
    spark-submit --master local[*] spark_streaming.py --duration 999999 --trigger-interval 30 > /app/data/spark_streaming.log 2>&1 &
else
    echo "Spark Structured Streaming is already running."
fi

# Execute the default container command (starts the Flask app)
exec "$@"
