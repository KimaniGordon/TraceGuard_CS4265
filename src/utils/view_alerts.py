import happybase
import pandas as pd
from datetime import datetime

def summarize_alerts():
    print(f"--- [TRACEGUARD SPEED LAYER: ALERT SUMMARY] ---")
    
    try:
        # Connect to the HBase Serving Layer
        connection = happybase.Connection('localhost', port=9090)
        
        # Check if the alerts table exists
        if b'alerts' not in connection.tables():
            print("[INFO] No 'alerts' table found in HBase. Run a stream match first!")
            return

        table = connection.table('alerts')
        
        # Pull all alerts from HBase
        alerts_data = []
        for key, data in table.scan():
            alerts_data.append({
                "Alert_ID": key.decode('utf-8'),
                "Src_IP": data.get(b'cf:src_ip', b'N/A').decode('utf-8'),
                "Threat": data.get(b'cf:description', b'N/A').decode('utf-8'),
                "Timestamp": data.get(b'cf:timestamp', b'N/A').decode('utf-8')
            })

        if not alerts_data:
            print("[INFO] The alerts table is currently empty.")
            return

        # Use Pandas for a clean display
        df = pd.DataFrame(alerts_data)
        
        print(f"[SUCCESS] Found {len(df)} real-time detections in HBase.")
        print("\n--- RECENT SECURITY EVENTS ---")
        print(df[['Timestamp', 'Src_IP', 'Threat']].sort_values(by='Timestamp').tail(10).to_string(index=False))
        
    except Exception as e:
        print(f"[ERROR] Could not query HBase alerts: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    summarize_alerts()