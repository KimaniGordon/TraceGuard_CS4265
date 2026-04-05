import os
from dotenv import load_dotenv

load_dotenv()

# --- Connection & Secrets ---
HDFS_HOST = os.getenv("HDFS_HOST", "localhost:9000")
OTX_KEY = os.getenv("OTX_API_KEY")
VT_KEY = os.getenv("VT_API_KEY")

# --- Root Directory Logic ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# --- Local Project Paths ---
RAW_INTEL_PATH = os.path.join(BASE_DIR, "data", "raw", "threat_intel_raw.csv")
TRAFFIC_INPUT_DIR = os.path.join(BASE_DIR, "data", "raw", "network_traffic")
INTEL_PARQUET_DIR = os.path.join(BASE_DIR, "data", "processed", "threat_indicators.parquet")
ALERTS_OUTPUT_DIR = os.path.join(BASE_DIR, "data", "processed", "alerts")
CHECKPOINT_DIR = os.path.join(BASE_DIR, "data", "checkpoints")

# --- HDFS Cluster Paths (Virtual Paths) ---
# Note: These don't use os.path.join because HDFS always uses forward slashes
HDFS_TRAFFIC_PATH = f"hdfs://{HDFS_HOST}/traceguard/raw/traffic/"