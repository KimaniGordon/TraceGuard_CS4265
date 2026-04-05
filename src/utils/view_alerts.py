import pandas as pd
import os
# Pulling the alert path from your central config
from src.config import ALERTS_OUTPUT_DIR

def summarize_alerts():
    print(f"--- TraceGuard Alert Summary ---")
    
    if not os.path.exists(ALERTS_OUTPUT_DIR) or not os.listdir(ALERTS_OUTPUT_DIR):
        print("[INFO] No alerts generated yet. Is the stream running?")
        return

    try:
        # Read the generated Parquet files into a dataframe
        df = pd.read_parquet(ALERTS_OUTPUT_DIR)
        
        if df.empty:
            print("[INFO] Alert folder exists but contains no records.")
        else:
            print(f"[SUCCESS] Found {len(df)} total alert triggers.")
            print("\nRecent Hits:")
            # Display the most important columns you selected in the stream
            print(df[['Src IP', 'Label', 'Timestamp']].tail(10))
            
    except Exception as e:
        print(f"[ERROR] Could not read alerts: {e}")

if __name__ == "__main__":
    summarize_alerts()