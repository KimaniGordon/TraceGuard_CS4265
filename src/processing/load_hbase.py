import happybase
import pandas as pd
import os
# Import our central configuration
from src.config import HDFS_HOST, INTEL_PARQUET_DIR

def load_to_hbase():
    # Extract just the hostname from "localhost:9000" (gives 'localhost')
    # This ensures HBase connects to the same machine as HDFS
    thrift_host = HDFS_HOST.split(':')[0] 
    
    print(f"[INFO] Connecting to HBase Thrift Gateway ({thrift_host}:9090)...")
    
    try:
        # 1. Initialize Connection using Config
        connection = happybase.Connection(
            host=thrift_host, 
            port=9090, 
            timeout=120000,
            protocol='binary',
            transport='buffered',
            autoconnect=False
        )
        
        print("[INFO] Handshaking with Thrift Gateway...")
        connection.open()
        
        # 2. Table Setup
        tables = [t.decode('utf-8') for t in connection.tables()]
        if 'threat_intel' not in tables:
            connection.create_table('threat_intel', {'cf': dict()})
            print("[INFO] Created new table: 'threat_intel'")

        table = connection.table('threat_intel')
        
        # 3. Path Check (Using Central Config)
        if not os.path.exists(INTEL_PARQUET_DIR):
            print(f"[ERROR] Could not find {INTEL_PARQUET_DIR}.")
            print("Tip: Run your Spark Engine script first to generate this data.")
            return

        # 4. Load Data from Parquet
        df = pd.read_parquet(INTEL_PARQUET_DIR)
        print(f"[INFO] Ingesting {len(df)} indicators into HBase...")

        # 5. Batch Load (Optimized for Big Data)
        print(f"[INFO] Ingesting {len(df)} rows into HBase...")
        with table.batch(batch_size=1000) as b:
            for _, row in df.iterrows():
                # 1. Define the Row Key (The unique identifier)
                indicator_key = str(row['indicator']).encode()
                
                # 2. Put all attributes under the 'cf' Column Family
                b.put(indicator_key, {
                    b'cf:type': str(row['type']).encode(),
                    b'cf:description': str(row['description']).encode(),
                    b'cf:processed_at': str(row['processed_at']).encode()
                })
        
        print("[SUCCESS] Data transformation and load complete.")
        
        print(f"[SUCCESS] TraceGuard Serving Layer populated with {len(df)} entries.")

    except Exception as e:
        print(f"[ERROR] HBase Load Failed: {e}")
        print("Checklist:")
        print("1. Is 'hbase thrift start' running?")
        print(f"2. Is the host '{thrift_host}' reachable?")
    finally:
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    load_to_hbase()