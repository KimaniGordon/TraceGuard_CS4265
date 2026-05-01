import os
import requests

def fetch_sample_data():
    """Automates the download of the 66.8 MB HDFS sample for M4 Validation"""
    # Using the Raw GitHub link for direct data access
    url = "https://raw.githubusercontent.com/KimaniGordon/TraceGuard_CS4265/main/data/sample/HDFS_sample.log"
    # Ensure this matches where your HDFS upload script looks for data
    target_dir = "data/raw/" 
    save_path = os.path.join(target_dir, "HDFS_large.log")

    if not os.path.exists(save_path):
        os.makedirs(target_dir, exist_ok=True)
        print(f"\n[M4 VALIDATION] Fetching sample dataset (66.8 MB)...")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print("[SUCCESS] Sample data secured. Ready for HDFS Landing.")
    else:
        print("\n[INFO] Data already present in data/raw/. Skipping fetch.")