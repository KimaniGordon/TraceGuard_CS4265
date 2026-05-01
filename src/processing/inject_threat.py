import pandas as pd
import os
from src.config import TRAFFIC_INPUT_DIR, RAW_DATA_DIR

def inject():
    traffic_file = os.path.join(TRAFFIC_INPUT_DIR, "aws_subset.csv")
    otx_file = os.path.join(RAW_DATA_DIR, "threat_intel_raw.csv")
    
    if not os.path.exists(traffic_file) or not os.path.exists(otx_file):
        print(f"[ERROR] Files missing. Skipping injection.")
        return

    print("[INFO] Performing Bulk Multi-Vector Injection...")
    df = pd.read_csv(traffic_file)
    otx_df = pd.read_csv(otx_file)
    
    # 1. Initialize default benign values for the whole dataset
    df['src_ip'] = '192.168.1.100'
    df['file_hash'] = 'CLEAN_PAYLOAD'

    # 2. Extract specific indicators from your OTX data
    malicious_ips = otx_df[otx_df['type'] == 'IPv4']['indicator'].head(3).tolist()
    malicious_hashes = otx_df[otx_df['type'].str.contains('Hash', case=False, na=False)]['indicator'].head(3).tolist()

    new_threat_rows = []

    # 3. Inject IP Threats (converting rows to dicts to avoid 3D shape errors)
    for ip in malicious_ips:
        row_dict = df.iloc[0].to_dict() # Get a single row as a dictionary
        row_dict['src_ip'] = ip
        row_dict['Label'] = 'Malicious_Network_Flow'
        new_threat_rows.append(row_dict)

    # 4. Inject Hash Threats
    for h in malicious_hashes:
        row_dict = df.iloc[1].to_dict()
        row_dict['file_hash'] = h
        row_dict['Label'] = 'Malicious_File_Transfer'
        new_threat_rows.append(row_dict)

    # 5. Combine and Save (This now passes 2D data correctly)
    threat_df = pd.DataFrame(new_threat_rows)
    final_df = pd.concat([threat_df, df], ignore_index=True)
    
    final_df.to_csv(traffic_file, index=False)
    print(f"[SUCCESS] Injected {len(malicious_ips)} IPs and {len(malicious_hashes)} Hashes into 2D schema.")

if __name__ == "__main__":
    inject()