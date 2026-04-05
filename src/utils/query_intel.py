import happybase
import sys
# Using the host configuration from central config
from src.config import HDFS_HOST

def lookup_indicator(indicator):
    thrift_host = HDFS_HOST.split(':')[0]
    
    try:
        # Establish connection to the Thrift Gateway
        connection = happybase.Connection(host=thrift_host, port=9090)
        table = connection.table('threat_intel')
        
        # Retrieve the row for the specific indicator
        row = table.row(indicator.encode())
        
        if row:
            # Decode the byte-data back to a readable string
            intel_type = row.get(b'intel:type').decode()
            print(f"\n[MATCH FOUND]")
            print(f"Indicator: {indicator}")
            print(f"Threat Type: {intel_type}")
        else:
            print(f"\n[CLEAN] No records found for: {indicator}")
            
    except Exception as e:
        print(f"[ERROR] HBase Query Failed: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.utils.query_intel <IP_OR_DOMAIN>")
    else:
        lookup_indicator(sys.argv[1])