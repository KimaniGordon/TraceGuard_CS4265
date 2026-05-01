import happybase
import pandas as pd
import sys

def scan_serving_layer(limit=100):
    try:
        # 1. Connect to Thrift
        connection = happybase.Connection('localhost', port=9090)
        table = connection.table('threat_intel')

        print(f"\n--- [TRACEGUARD: SCANNING {limit} ROWS FROM SERVING LAYER] ---")
        
        data = []
        # 2. Scan with a high limit
        for key, cells in table.scan(limit=limit):
            data.append({
                "Indicator": key.decode('utf-8'),
                "Type": cells[b'cf:type'].decode('utf-8'),
                "Description": cells[b'cf:description'].decode('utf-8')
            })

        if not data:
            print("[!] No data found in HBase table 'threat_intel'.")
            return

        # 3. Create DataFrame
        df = pd.DataFrame(data)

        # --- THE CLEANING LOGIC (Prevents 'Wonky' Output) ---
        # Shorten the Hashes and Descriptions so they fit on one terminal line
        df['Indicator'] = df['Indicator'].apply(lambda x: (x[:20] + '..') if len(x) > 20 else x)
        df['Description'] = df['Description'].apply(lambda x: x.replace('\n', ' ').strip()[:60] + "...")

        # Force Pandas to show everything in a grid
        pd.set_option('display.max_rows', None)      # Show all rows in the batch
        pd.set_option('display.max_columns', None)   # Show all columns
        pd.set_option('display.width', 1000)         # Prevent wrapping
        pd.set_option('display.colheader_justify', 'left')

        # 4. Print the clean table
        print(df.to_string(index=False))
        print(f"\n[SUMMARY] Displayed {len(df)} records.")

    except Exception as e:
        print(f"[ERROR] Could not connect to HBase. Is Thrift running?\n{e}")

if __name__ == "__main__":
    # You can change the number here to 500 or 1000 if you want to see more!
    scan_serving_layer(limit=200)