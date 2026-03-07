import os
import requests
import pandas as pd
from dotenv import load_dotenv

# 1. Load your .env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
API_KEY = os.getenv("OTX_API_KEY")

def ingest_siphon():
    if not API_KEY:
        print("❌ ERROR: OTX_API_KEY is missing from your .env file.")
        return

    print("📡 TraceGuard: Siphoning subscribed pulses to CSV...")
    
    # This is the Community-safe endpoint (The 'OTX Siphon' method)
    url = "https://otx.alienvault.com/api/v1/pulses/subscribed"
    headers = {"X-OTX-API-KEY": API_KEY}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        pulses = response.json().get('results', [])
        
        if not pulses:
            print("⚠️ No data found. Did you 'Follow' any pulses (SQLi/Malware) on OTX?")
            return

        # Flattening indicators for your Spark-ready CSV
        data_list = []
        for pulse in pulses:
            for ind in pulse.get('indicators', []):
                data_list.append({
                    "indicator": ind.get('indicator'),
                    "type": ind.get('type'),
                    "pulse_title": pulse.get('name'),
                    "description": pulse.get('description', 'N/A')
                })
        
        df = pd.DataFrame(data_list)
        
        # 2. Ensure the 'data/raw' folder exists
        output_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
        os.makedirs(output_dir, exist_ok=True)
        
        # 3. Save the file
        output_file = os.path.join(output_dir, 'threat_sample.csv')
        df.to_csv(output_file, index=False)
        
        print(f"✅ SUCCESS! Ingested {len(df)} indicators to {output_file}")

    except Exception as e:
        print(f"❌ Ingestion Failed: {e}")

if __name__ == "__main__":
    ingest_siphon()