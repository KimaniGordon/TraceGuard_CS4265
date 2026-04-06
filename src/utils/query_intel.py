import happybase
import sys

def query_indicator(indicator):
    try:
        # Connect to HBase
        connection = happybase.Connection('localhost', port=9090)
        table = connection.table('threat_intel')

        # Get the row for the specific indicator
        row = table.row(indicator.encode())

        if not row:
            print(f"\n[!] NO MATCH FOUND for: {indicator}")
            return

        # Use .get() with a default empty byte string to prevent NoneType errors
        # Note: using 'cf' as the column family prefix for all attributes
        type_val = row.get(b'cf:type', b'N/A').decode()
        desc_val = row.get(b'cf:description', b'No description available').decode()
        proc_val = row.get(b'cf:processed_at', b'Unknown').decode()

        print("\n" + "="*60)
        print(f"  TRACEGUARD INTELLIGENCE MATCH")
        print("="*60)
        print(f"INDICATOR:    {indicator}")
        print(f"TYPE:         {type_val}")
        print(f"DESCRIPTION:  {desc_val}")
        print(f"PROCESSED AT: {proc_val}")
        print("="*60 + "\n")

        connection.close()

    except Exception as e:
        print(f"[ERROR] HBase Query Failed: {e}")
        print("Tip: Ensure 'hbase thrift start -p 9090' is running.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.utils.query_intel <IP_OR_HASH>")
    else:
        query_indicator(sys.argv[1])