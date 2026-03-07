import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

# 1. Pipeline Framework: Load secure credentials
load_dotenv()
API_KEY = os.getenv("OTX_API_KEY")

def run_ingestion():
    print(f"[{datetime.now()}] 🚀 TraceGuard: Starting Ingestion...")
    
    # AlienVault OTX Endpoint for 'General' indicator pulses
    # This pulls recent threat reports shared by the community
    url = "https://otx.alienvault.com/api/v1/pulses/subscribed"
    headers = {"X-OTX-API-KEY": API_KEY}

    try:
        # 2. Acquisition: Real-time API Call
        print(f"[{datetime.now()}] 📡 Connecting to AlienVault OTX...")
        response = requests.get(url, headers=headers, params={"limit": 20})
        response.raise_for_status()
        
        # 3. Processing: Convert JSON to a Data Structure
        data = response.json()
        pulses = data.get('results', [])
        
        # 4. Persistence: Save to local storage (CSV/Parquet)
        if pulses:
            df = pd.DataFrame(pulses)
            # Ensure the directory exists
            os.makedirs("data/raw", exist_ok=True)
            
            output_path = "data/raw/threat_feed_sample.csv"
            df.to_csv(output_path, index=False)
            
            print(f"[{datetime.now()}] ✅ Success! Ingested {len(df)} threat pulses.")
            print(f"[{datetime.now()}] 💾 Data persisted to: {output_path}")
        else:
            print(f"[{datetime.now()}] ⚠️ No new data found in the subscription feed.")

    except Exception as e:
        print(f"[{datetime.now()}] ❌ Critical Error: {e}")

if __name__ == "__main__":
    run_ingestion()