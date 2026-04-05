import os
import requests
import pandas as pd
from dotenv import load_dotenv

def run_ingestion():
    load_dotenv()
    API_KEY = os.getenv("OTX_API_KEY")

    if not API_KEY:
        print("[ERROR] OTX_API_KEY not found in .env")
        return None
    
    print("[INFO] Fetching threat indicators from OTX...")
    url = "https://otx.alienvault.com/api/v1/pulses/subscribed"
    headers = {"X-OTX-API-KEY": API_KEY}
    
    try:
        # Added timeout and error checking
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        pulses = response.json().get("results", [])
        
        all_indicators = []
        for p in pulses:
            p_name = p.get('name', 'Unknown')
            # "Exploding" indicators to move beyond parent metadata 
            indicators = p.get('indicators', [])
            for ind in indicators:
                all_indicators.append({
                    "indicator": ind.get('indicator'),
                    "type": ind.get('type'),
                    "pulse_name": p_name,
                    "created": ind.get('created')
                })
        
        df = pd.DataFrame(all_indicators)
        if not df.empty:
            os.makedirs("data/raw", exist_ok=True)
            output_path = "data/raw/threat_intel_raw.csv"
            df.to_csv(output_path, index=False)
            print(f"[INFO] Ingested {len(df)} atomic indicators to {output_path}")
            return output_path
        else:
            print("[WARN] No indicators found in subscribed pulses.")
            return None
            
    except Exception as e:
        print(f"[ERROR] Ingestion failed: {e}")
        return None
    
if __name__ == "__main__":
    run_ingestion()
    print("[SUCCESS] Threat intelligence downloaded.")