import happybase
import sys

def search_by_type(search_term):
    """Scans HBase for threats matching a specific keyword in the description."""
    try:
        connection = happybase.Connection('localhost', port=9090)
        table = connection.table('threat_intel')
        
        print(f"\n[INVESTIGATION] Searching for threat signature: '{search_term}'...")
        print("-" * 75)
        print(f"{'MATCHING IP':<20} | {'DESCRIPTION'}")
        print("-" * 75)

        # We use a scan with a filter (case-insensitive check)
        count = 0
        for key, data in table.scan():
            desc = data.get(b'cf:description', b'').decode().lower()
            if search_term.lower() in desc:
                print(f"{key.decode():<20} | {data[b'cf:description'].decode()[:50]}...")
                count += 1
                if count >= 15: # Cap it so the screen doesn't explode
                    print("\n[INFO] Showing first 15 results...")
                    break
        
        if count == 0:
            print(f"[CLEAN] No threats found matching '{search_term}'.")
        else:
            print(f"\n[SUCCESS] Found {count} matching threat(s).")
            
        connection.close()
    except Exception as e:
        print(f"[ERROR] Search failed: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.utils.search_threats <KEYWORD>")
    else:
        search_by_type(sys.argv[1])