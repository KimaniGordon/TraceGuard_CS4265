import os
from dotenv import load_dotenv

load_dotenv()

# --- Connection & Secrets ---
HDFS_HOST = os.getenv("HDFS_HOST", "localhost:9000")
OTX_KEY = os.getenv("OTX_API_KEY")
VT_KEY = os.getenv("VT_API_KEY")

# --- Remote Data Sources ---
SAMPLE_DATA_URL = "https://raw.githubusercontent.com/KimaniGordon/TraceGuard_CS4265/main/data/sample/HDFS_sample.log"

# --- Root Directory Logic ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# --- Local Project Paths ---
# 1. Define the RAW folder (This was the missing piece!)
RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw") 

# 2. Define specific file/folder paths
RAW_INTEL_PATH = os.path.join(RAW_DATA_DIR, "threat_intel_raw.csv")
TRAFFIC_INPUT_DIR = os.path.join(RAW_DATA_DIR, "network_traffic")

# 3. Define processed data paths
INTEL_PARQUET_DIR = os.path.join(BASE_DIR, "data", "processed", "threat_indicators.parquet")
ALERTS_OUTPUT_DIR = os.path.join(BASE_DIR, "data", "processed", "alerts")
CHECKPOINT_DIR = os.path.join(BASE_DIR, "data", "checkpoints")

# --- HDFS Cluster Paths ---
HDFS_TRAFFIC_PATH = f"hdfs://{HDFS_HOST}/traceguard/raw/traffic/"