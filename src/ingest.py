import os, requests, pandas as pd
from dotenv import load_dotenv

# Check root and current directory for .env
load_dotenv(".env")
load_dotenv("../.env")

API_KEY = os.getenv("OTX_API_KEY")

def run():
    if not API_KEY:
        #  use a raw string (r"") here to prevent the backslash error
        print(r"Error: OTX_API_KEY still not found. Check .env in C:\Users\gordo\TraceGuard")
        return
    
    print(" TraceGuard: Connecting to OTX Community API...")
    url = "https://otx.alienvault.com/api/v1/pulses/subscribed"
    headers = {"X-OTX-API-KEY": API_KEY}
    
    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json().get("results", [])
        if data:
            df = pd.DataFrame([{"id": p["id"], "name": p["name"]} for p in data])
            # Save to landing zone
            os.makedirs("data/raw", exist_ok=True)
            df.to_csv("data/raw/threat_sample.csv", index=False)
            print(f"Success! Ingested {len(df)} threat indicators.")
        else:
            print("No data found. Follow pulses on OTX website first!")
    except Exception as e:
        print(f" Failed: {e}")

if __name__ == "__main__":
    run()
