import sys
import os
import psycopg2
import pandas as pd

def print_help():
    print("PostgreSQL → HDFS Export Tool")
    print("Usage: python sqoop_ingest.py import --connect <jdbc_url> --table <table_name> --target-dir <hdfs_path> --username <user> --password <pass>")

def main():
    print("--- Executing PostgreSQL → HDFS Export ---")
    print("Arguments passed:", sys.argv)
    
    # Parse CLI parameters
    args = sys.argv
    if len(args) < 2 or args[1] != 'import':
        print_help()
        sys.exit(1)
        
    connect_str = None
    table_name = None
    target_dir = None
    username = None
    password = None
    
    i = 2
    while i < len(args):
        if args[i] == '--connect':
            connect_str = args[i+1]
            i += 2
        elif args[i] == '--table':
            table_name = args[i+1]
            i += 2
        elif args[i] == '--target-dir':
            target_dir = args[i+1]
            i += 2
        elif args[i] == '--username':
            username = args[i+1]
            i += 2
        elif args[i] == '--password':
            password = args[i+1]
            i += 2
        else:
            i += 1
            
    if not connect_str or not table_name or not target_dir:
        print("Error: Missing required Sqoop parameters.")
        print_help()
        sys.exit(1)
        
    print(f"Connecting to database via JDBC bridge: {connect_str}...")
    print(f"Importing table: {table_name}...")
    print(f"Destination HDFS path: {target_dir}...")
    
    # Map JDBC URI to python psycopg2 params
    # Example: jdbc:postgresql://postgres-db:5432/crop_yield_db -> postgresql://postgres:postgres@postgres-db:5432/crop_yield_db
    # We will read directly from the database using psycopg2.
    
    # Extract host, port, dbname from jdbc string
    # jdbc:postgresql://postgres-db:5432/crop_yield_db
    try:
        clean_url = connect_str.replace("jdbc:postgresql://", "").replace("jdbc:postgres://", "")
        host_port, dbname = clean_url.split('/')
        if ':' in host_port:
            host, port = host_port.split(':')
        else:
            host = host_port
            port = "5432"
            
        print(f"Extracted Params: Host={host}, Port={port}, DB={dbname}")
        
        # Connect
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=dbname,
            user=username or "postgres",
            password=password or "postgres"
        )
        
        # Read table into pandas
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, conn)
        conn.close()
        
        print(f"Imported {len(df)} rows from PostgreSQL database.")
        
        # Save to local temporary file
        temp_csv = f"/tmp/{table_name}_sqoop.csv"
        df.to_csv(temp_csv, index=False, header=False) # Sqoop typically exports headerless csv files
        print(f"Saved temporary table dump to {temp_csv}")
        
        # Upload to HDFS target-dir via WebHDFS
        import requests
        print(f"Creating HDFS directory: {target_dir}")
        requests.put(f"http://namenode:9870/webhdfs/v1{target_dir}?op=MKDIRS")

        hdfs_url = f"http://namenode:9870/webhdfs/v1{target_dir}/{table_name}.csv?op=CREATE&overwrite=true"
        print(f"Uploading via WebHDFS: {hdfs_url}")
        r = requests.put(hdfs_url, allow_redirects=False)
        if r.status_code == 307:
            redirect_url = r.headers['Location']
            # Upload actual data to datanode
            with open(temp_csv, 'rb') as f:
                r2 = requests.put(redirect_url, data=f)
            if r2.status_code in (200, 201):
                print("WebHDFS upload completed successfully.")
            else:
                raise Exception(f"Failed to write to datanode: {r2.status_code} {r2.text}")
        else:
            raise Exception(f"Failed to initiate WebHDFS write: {r.status_code} {r.text}")
        
        # Clean up local file
        if os.path.exists(temp_csv):
            os.remove(temp_csv)
            
        print(f"Export completed! Data successfully imported into HDFS: {target_dir}/{table_name}.csv")
        
    except Exception as e:
        print("Export failed:", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
