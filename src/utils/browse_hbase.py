import happybase
#Database browsing utility for HBase. This is a simple script that connects to the HBase Thrift server, retrieves a sample of rows from the 'threat_intel' table, and prints them in a structured format. This can be used for quick diagnostics to verify that data is being stored correctly in HBase. 
def run_browse(limit=20):
    """
    Connects to HBase via Thrift and prints a structured sample of the threat intelligence.
    """
    try:
        connection = happybase.Connection('localhost', port=9090)
        table = connection.table('threat_intel')

        print("\n" + "="*60)
        print("   TRACEGUARD DIAGNOSTIC: HBASE SERVING LAYER CONTENT")
        print("="*60)
        print(f"{'ROW KEY (IP)':<20} | {'TYPE':<10} | {'DESCRIPTION'}")
        print("-" * 75)

        for key, data in table.scan(limit=limit):
            ip = key.decode()
            # Handle potential missing fields gracefully
            t_type = data.get(b'cf:type', b'unknown').decode()
            desc = data.get(b'cf:description', b'no description provided').decode()
            
            print(f"{ip:<20} | {t_type:<10} | {desc[:40]}...")

        print("="*60)
        connection.close()

    except Exception as e:
        print(f"[ERROR] Could not browse HBase: {e}")

if __name__ == "__main__":
    
    run_browse()