import happybase
import pandas as pd
import sys

def run_browse(limit=100):
    """
    Renamed to run_browse to match main.py expectations.
    Displays a clean, grid-view of the HBase Serving Layer.
    """
    try:
        # 1. Connect to Thrift (Ensure 'hbase thrift start -p 9090' is running!)
        connection = happybase.Connection('localhost', port=9090)
        table = connection.table('threat_intel')

        print(f"\n--- [TRACEGUARD: SCANNING {limit} ROWS FROM SERVING LAYER] ---")
        
        data = []
        # 2. Scan with the provided limit
        for key, cells in table.scan(limit=limit):
            #  use .get() to prevent KeyErrors if a record is partially missing data
            data.append({
                "Indicator": key.decode('utf-8'),
                "Type": cells.get(b'cf:type', b'N/A').decode('utf-8'),
                "Description": cells.get(b'cf:description', b'N/A').decode('utf-8')
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
        pd.set_option('display.max_rows', None)      
        pd.set_option('display.max_columns', None)   
        pd.set_option('display.width', 1000)         
        pd.set_option('display.colheader_justify', 'left')

        # 4. Print the clean table
        print(df.to_string(index=False))
        print(f"\n[SUMMARY] Displayed {len(df)} records.")

        connection.close()

    except Exception as e:
        print(f"[ERROR] HBase Browse Failed. Is Thrift running on 9090?\nDetail: {e}")

if __name__ == "__main__":
    # If run manually, it defaults to 200 rows
    run_browse(limit=200)