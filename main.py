import os
import sys
import subprocess

# --- 1. IMPORT ALL WRAPPERS FROM THE SCRIPTS ---
# Ingestion Layer
from src.processing import inject_threat
from src.utils.data_fetcher import fetch_sample_data
from src.ingestion.fetch_otx import run_ingestion as fetch_otx
from src.ingestion.fetch_aws import download_aws_subset as fetch_aws
from src.ingestion.load_data import upload_to_hdfs as upload_logs_to_hdfs

# Processing & Serving Layer
from src.processing.data_cleanse import deep_clean
from src.processing.spark_engine import run_transformations
from src.processing.process_hdfs import process_hdfs_logs
from src.processing.load_hbase import load_to_hbase
from src.processing.stream_correlation import start_streaming
from src.processing.inject_threat import inject as inject

# Configuration
from src.config import TRAFFIC_INPUT_DIR

def upload_traffic_to_hdfs():
    """Helper to move the local AWS traffic into the HDFS Speed Layer directory"""
    # Using 'hadoop fs -put' via subprocess just like load_data.py
    local_traffic = os.path.join(TRAFFIC_INPUT_DIR, "aws_subset.csv")
    local_traffic_abs = os.path.abspath(local_traffic)
    
    print(f"[INFO] Uploading {local_traffic} to HDFS Speed Layer...")
    subprocess.run(["hadoop", "fs", "-mkdir", "-p", "/traceguard/raw/traffic/"], shell=True)
    subprocess.run(["hadoop", "fs", "-put", "-f", local_traffic_abs, "/traceguard/raw/traffic/"], shell=True)

def run_traceguard_pipeline():
    print("\n" + "="*60)
    print("  TRACEGUARD: UNIFIED BIG DATA IDS PIPELINE")
    print("="*60)

    # STAGE 0.5: SAMPLE DATA FETCH
    # This is just to get the sample data for schema inference and testing
    print("\n--- [STAGE 0.5: SAMPLE DATA FETCH] ---")
    fetch_sample_data()

    # STAGE 0: CLEANUP
    # Reset the environment to ensure a fresh, successful run
    #deep_clean()

    # STAGE 1: INGESTION
    # Fetching raw data from APIs and AWS Registry
    print("\n--- [STAGE 1: DATA INGESTION] ---")
    #fetch_otx()
    #fetch_aws()

    # STAGE 1.5: CANARY THREAT INJECTION
    # This is a critical step to validate the entire pipeline's ability to detect and process threats
    print("\n--- [STAGE 1.5: CANARY THREAT INJECTION] ---")
    inject()

    # STAGE 2: STORAGE (HDFS LANDING)
    # Moving local files into the distributed cluster
    print("\n--- [STAGE 2: HDFS CLUSTER LANDING] ---")
    upload_logs_to_hdfs()
    upload_traffic_to_hdfs()

    # STAGE 3: BATCH PROCESSING
    # Using Spark to normalize indicators and parse massive logs
    print("\n--- [STAGE 3: SPARK BATCH PROCESSING] ---")
    run_transformations()
    process_hdfs_logs()

    # --- [STAGE 4: HBASE SERVING LAYER LOAD] ---
    # Populating HBase for fast lookups
    print("\n--- [STAGE 4: HBASE SERVING LAYER LOAD] ---")
    
    # Imports moved to the top of the file usually, but added here for personal prefrence
    from src.utils.browse_hbase import run_browse 
    
    load_to_hbase()  # Runs the ingestion into HBase
    
    # Updated to 25 to show the "Big Data" scale and variety of threats
    print("\n[RESULTS] Previewing HBase Serving Layer (Random Sample of 25 Records)...")
    run_browse(limit=40)

    # STAGE 5: SPEED LAYER (STREAMING)
    # This is the 'Blocking' step - it stays active until you stop it
    print("\n--- [STAGE 5: REAL-TIME SPEED LAYER ACTIVE] ---")
    print("Simulation active. Monitoring traffic for threat matches...")
    start_streaming()

if __name__ == "__main__":
    try:
        run_traceguard_pipeline()
    except KeyboardInterrupt:
        print("\n\n TraceGuard Shutdown by User. Exiting.")
        sys.exit(0)
    except Exception as e:
        print(f"\n Pipeline Failed: {e}")

        # Ctrl C interrupt is expected when stopping the streaming layer, a pipeline failed message may occur but it's not an actual error. You can ignore it if you see it after stopping the stream.